/*
 *  Copyright 2018 Kjell Winblad (kjellwinblad@gmail.com, http://winsh.me)
 *
 *  This file is part of catrees.
 *
 *  catrees is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  catrees is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with catrees.  If not, see <http://www.gnu.org/licenses/>.
 */


package me.winsh.ds.concurrent.catrees;

import java.io.File;
import java.io.PrintStream;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import me.winsh.ds.immutable.ImmutableTreapMap;
import me.winsh.ds.immutable.ImmutableTreapMap.ImmutableTreapValue;
import me.winsh.ds.mutable.ImmutableTreapMapHolder;
import me.winsh.ds.mutable.LongStack;
import me.winsh.ds.mutable.Stack;
import me.winsh.ds.mutable.interfaces.SplitableAndJoinableMap;
/**
 * This is an implementation of the data structure called the
 * contention adapting search tree which is described in the following
 * paper:
 * 
 * A Contention Adapting Approach to Concurrent Ordered Sets
 * Journal of Parallel and Distributed Computing
 * 2017
 * https://doi.org/10.1016/j.jpdc.2017.11.007
 * 
 * The implementation is using an optimization that is described in:
 * 
 * Faster Concurrent Range Queries with Contention Adapting Search Trees Using Immutable Data
 * In the proceedings of 2017 Imperial College Computing Student Workshop (ICCSW 2017)
 * https://arxiv.org/abs/1709.00722
 * http://dx.doi.org/10.4230/OASIcs.ICCSW.2017.7
 * 
 * The last paper is also a good source for seeing how this particular
 * implementation perform compared to other data structures.
 * 
 * @author Kjell Winblad (kjellwinblad@gmail.com, http://winsh.me)
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ImmDataCATreeMap<K, V> extends AbstractMap<K, V> implements ConcurrentNavigableMap<K, V> {
	
	// === Private functions and data structures ===
	
	private volatile Object root = new ImmutableTreapMapHolder<K, V>();
	
	private final Comparator<? super K> comparator;

	static private final class RouteNode {
		volatile Object left;
		volatile Object right;
		final Object key;
		final ReentrantLock lock = new ReentrantLock();
		boolean valid = true;

		public RouteNode(Object key, Object left, Object right) {
			this.key = key;
			this.left = left;
			this.right = right;
		}

		public String toString() {
			return "R(" + key + ")";
		}
	}



	
	//TODO rewrite this to make use of snapshot functionality
	private int sizeHelper(Object currentNode) {
		if (currentNode == null) {
			return 0;
		} else {
			if (currentNode instanceof RouteNode) {
				RouteNode r = (RouteNode) currentNode;
				int sizeSoFar = sizeHelper(r.left);
				return sizeSoFar + sizeHelper(r.right);
			} else {
				@SuppressWarnings("unchecked")
				ImmutableTreapMapHolder<K, V> b = (ImmutableTreapMapHolder<K, V>) currentNode;
				return b.size();
			}
		}
	}

	final private Object getBaseNodeUsingComparator(Object keyParam) {
		Object currNode = root;
		@SuppressWarnings("unchecked")
		K key = (K) keyParam;
		while (currNode instanceof RouteNode) {
			RouteNode currNodeR = (RouteNode) currNode;
			@SuppressWarnings("unchecked")
			K routeKey = (K) (currNodeR.key);
			if (comparator.compare(key, routeKey) < 0) {
				currNode = currNodeR.left;
			} else {
				currNode = currNodeR.right;
			}
		}
		return currNode;
	}

	final private Object getBaseNode(Object keyParam) {
		Object currNode = root;
		if (comparator != null) {
			return getBaseNodeUsingComparator(keyParam);
		} else {
			@SuppressWarnings("unchecked")
			Comparable<? super K> key = (Comparable<? super K>) keyParam;
			while (currNode instanceof RouteNode) {
				RouteNode currNodeR = (RouteNode) currNode;
				@SuppressWarnings("unchecked")
				K routeKey = (K) (currNodeR.key);
				if (key.compareTo(routeKey) < 0) {
					currNode = currNodeR.left;
				} else {
					currNode = currNodeR.right;
				}
			}
			return currNode;
		}
	}

	final private void highContentionSplit(ImmutableTreapMapHolder<K, V> baseNode) {
		if (baseNode.hasLessThanTwoElements()) {
			baseNode.resetStatistics();// Fast path out if nrOfElem <= 1
			return;
		}

		RouteNode parent = (RouteNode) baseNode.getParent();
		Object[] writeBackSplitKey = new Object[1];
		@SuppressWarnings("unchecked")
		SplitableAndJoinableMap<K, V>[] writeBackRightTree = new SplitableAndJoinableMap[1];
		ImmutableTreapMapHolder<K, V> leftTree = (ImmutableTreapMapHolder<K, V>) baseNode.split(writeBackSplitKey,
				writeBackRightTree);
		if (leftTree == null) {
			baseNode.resetStatistics();
			return;
		}
		@SuppressWarnings("unchecked")
		K splitKey = (K) writeBackSplitKey[0];
		ImmutableTreapMapHolder<K, V> rightTree = (ImmutableTreapMapHolder<K, V>) writeBackRightTree[0];
		RouteNode newRoute = new RouteNode(splitKey, leftTree, rightTree);
		leftTree.setParent(newRoute);
		rightTree.setParent(newRoute);
		if (parent == null) {
			root = newRoute;
		} else {
			if (parent.left == baseNode) {
				parent.left = newRoute;
			} else {
				parent.right = newRoute;
			}
		}
		baseNode.invalidate();
	}

	final private ImmutableTreapMapHolder<K, V> leftmostBaseNode(Object node) {
		Object currentNode = node;
		while (currentNode instanceof RouteNode) {
			RouteNode r = (RouteNode) currentNode;
			currentNode = r.left;
		}
		@SuppressWarnings("unchecked")
		ImmutableTreapMapHolder<K, V> toReturn = (ImmutableTreapMapHolder<K, V>) currentNode;
		return toReturn;
	}

	final private ImmutableTreapMapHolder<K, V> rightmostBaseNode(Object node) {
		Object currentNode = node;
		while (currentNode instanceof RouteNode) {
			RouteNode r = (RouteNode) currentNode;
			currentNode = r.right;
		}
		@SuppressWarnings("unchecked")
		ImmutableTreapMapHolder<K, V> toReturn = (ImmutableTreapMapHolder<K, V>) currentNode;
		return toReturn;
	}

	final private RouteNode parentOfUsingComparator(RouteNode node) {
		@SuppressWarnings("unchecked")
		K key = (K) node.key;
		Object prevNode = null;
		Object currNode = root;

		while (currNode != node) {
			RouteNode currNodeR = (RouteNode) currNode;
			@SuppressWarnings("unchecked")
			K routeKey = (K) (currNodeR.key);
			prevNode = currNode;
			if (comparator.compare(key, routeKey) < 0) {
				currNode = currNodeR.left;
			} else {
				currNode = currNodeR.right;
			}
		}
		return (RouteNode) prevNode;

	}

	final private RouteNode parentOf(RouteNode node) {
		if (comparator != null) {
			return parentOfUsingComparator(node);
		} else {
			@SuppressWarnings("unchecked")
			Comparable<? super K> key = (Comparable<? super K>) node.key;
			Object prevNode = null;
			Object currNode = root;
			while (currNode != node) {
				RouteNode currNodeR = (RouteNode) currNode;
				@SuppressWarnings("unchecked")
				K routeKey = (K) (currNodeR.key);
				prevNode = currNode;
				if (key.compareTo(routeKey) < 0) {
					currNode = currNodeR.left;
				} else {
					currNode = currNodeR.right;
				}
			}
			return (RouteNode) prevNode;
		}
	}

	final private void lowContentionJoin(ImmutableTreapMapHolder<K, V> baseNode) {
		RouteNode parent = (RouteNode) baseNode.getParent();
		if (parent == null) {
			baseNode.resetStatistics();
		} else if (parent.left == baseNode) {
			ImmutableTreapMapHolder<K, V> neighborBase = leftmostBaseNode(parent.right);
			if (!neighborBase.tryLock()) {
				baseNode.resetStatistics();
				return;
			} else if (!neighborBase.isValid()) {
				neighborBase.unlock();
				baseNode.resetStatistics();
				return;
			} else {
				// System.out.println("JOIN HAPPENS");
				ImmutableTreapMapHolder<K, V> newNeighborBase = (ImmutableTreapMapHolder<K, V>) baseNode
						.join(neighborBase);
				parent.lock.lock();
				RouteNode gparent = null; // gparent = grandparent
				do {
					if (gparent != null) {
						gparent.lock.unlock();
					}
					gparent = parentOf(parent);
					if (gparent != null) {
						gparent.lock.lock();
					}
				} while (gparent != null && !gparent.valid);
				if (gparent == null) {
					root = parent.right;
				} else if (gparent.left == parent) {
					gparent.left = parent.right;
				} else {
					gparent.right = parent.right;
				}
				parent.valid = false;
				parent.lock.unlock();
				if (gparent != null) {
					gparent.lock.unlock();
				}
				// Unlink is done!
				// Put in joined base node
				RouteNode neighborBaseParent = null;
				if (parent.right == neighborBase) {
					neighborBaseParent = gparent;
				} else {
					neighborBaseParent = (RouteNode) neighborBase.getParent();
				}
				newNeighborBase.setParent(neighborBaseParent);
				if (neighborBaseParent == null) {
					root = newNeighborBase;
				} else if (neighborBaseParent.left == neighborBase) {
					neighborBaseParent.left = newNeighborBase;
				} else {
					neighborBaseParent.right = newNeighborBase;
				}
				neighborBase.invalidate();
				neighborBase.unlock();
				baseNode.invalidate();
			}
		} else { /* This case is symmetric to the previous one */
			ImmutableTreapMapHolder<K, V> neighborBase = rightmostBaseNode(parent.left);// ff
			if (!neighborBase.tryLock()) {// ff
				baseNode.resetStatistics();// ff
			} else if (!neighborBase.isValid()) {// ff
				neighborBase.unlock();// ff
				baseNode.resetStatistics();// ff
			} else {
				// System.out.println("R" + baseNode + " " + neighborBase);
				ImmutableTreapMapHolder<K, V> newNeighborBase = (ImmutableTreapMapHolder<K, V>) neighborBase
						.join(baseNode);// ff
				parent.lock.lock();// ff
				RouteNode gparent = null; // gparent = grandparent //ff
				do {// ff
					if (gparent != null) {// ff
						gparent.lock.unlock();// ff
					} // ff
					gparent = parentOf(parent);// ff
					if (gparent != null) {// ff
						gparent.lock.lock();// ff
					} // ff
				} while (gparent != null && !gparent.valid);// ff
				if (gparent == null) {// ff
					root = parent.left;// ff
				} else if (gparent.left == parent) {// ff
					gparent.left = parent.left;// ff
				} else {// ff
					gparent.right = parent.left;// ff
				} // ff
				parent.valid = false;
				parent.lock.unlock();// ff
				if (gparent != null) {// ff
					gparent.lock.unlock();// ff
				} // ff
				RouteNode neighborBaseParent = null;
				if (parent.left == neighborBase) {
					neighborBaseParent = gparent;
				} else {
					neighborBaseParent = (RouteNode) neighborBase.getParent();
				}
				newNeighborBase.setParent(neighborBaseParent);// ff
				if (neighborBaseParent == null) {// ff
					root = newNeighborBase;// ff
				} else if (neighborBaseParent.left == neighborBase) {// ff
					neighborBaseParent.left = newNeighborBase;// ff
				} else {// ff
					neighborBaseParent.right = newNeighborBase;// ff
				} // ff
				neighborBase.invalidate();// ff
				neighborBase.unlock();// ff
				baseNode.invalidate();// ff
			}
		}
	}

	private final void adaptIfNeeded(ImmutableTreapMapHolder<K, V> baseNode) {
		if (baseNode.isHighContentionLimitReached()) {
			highContentionSplit(baseNode);
		} else if (baseNode.isLowContentionLimitReached()) {
			lowContentionJoin(baseNode);
		}
	}

	final private void addAllToList(Object currentNode, LinkedList<Map.Entry<K, V>> list) {
		if (currentNode == null) {
			return;
		} else {
			if (currentNode instanceof RouteNode) {
				RouteNode r = (RouteNode) currentNode;
				addAllToList(r.left, list);
				addAllToList(r.right, list);
			} else {
				@SuppressWarnings("unchecked")
				ImmutableTreapMapHolder<K, V> b = (ImmutableTreapMapHolder<K, V>) currentNode;
				b.addAllToList(list);
				return;
			}
		}
	}



	// boolean equals(Object o);
	// int hashCode();

	final private Object getBaseNodeAndStackUsingComparator(Object keyParam, Stack<RouteNode> stack) {
		Object currNode = root;
		@SuppressWarnings("unchecked")
		K key = (K) keyParam;
		while (currNode instanceof RouteNode) {
			RouteNode currNodeR = (RouteNode) currNode;
			stack.push(currNodeR);
			@SuppressWarnings("unchecked")
			K routeKey = (K) (currNodeR.key);
			if (comparator.compare(key, routeKey) < 0) {
				currNode = currNodeR.left;
			} else {
				currNode = currNodeR.right;
			}
		}
		return currNode;
	}

	@SuppressWarnings("unchecked")
	final private ImmutableTreapMapHolder<K, V> getBaseNodeAndStack(Object keyParam, Stack<RouteNode> stack) {
		Object currNode = root;
		if (comparator != null) {
			return (ImmutableTreapMapHolder<K, V>) getBaseNodeAndStackUsingComparator(keyParam, stack);
		} else {
			Comparable<? super K> key = (Comparable<? super K>) keyParam;
			while (currNode instanceof RouteNode) {
				RouteNode currNodeR = (RouteNode) currNode;
				stack.push(currNodeR);
				K routeKey = (K) (currNodeR.key);
				if (key.compareTo(routeKey) < 0) {
					currNode = currNodeR.left;
				} else {
					currNode = currNodeR.right;
				}
			}
			return (ImmutableTreapMapHolder<K, V>) currNode;
		}
	}

	final private ImmutableTreapMapHolder<K, V> leftmostBaseNodeAndStack(Object node, Stack<RouteNode> stack) {
		Object currentNode = node;
		while (currentNode instanceof RouteNode) {
			RouteNode r = (RouteNode) currentNode;
			stack.push(r);
			currentNode = r.left;
		}
		@SuppressWarnings("unchecked")
		ImmutableTreapMapHolder<K, V> toReturn = (ImmutableTreapMapHolder<K, V>) currentNode;
		return toReturn;
	}

	// final private ImmutableTreapMapHolder<K,V> getNextBaseNodeAndStack(Object
	// baseNode, Stack<RouteNode> stack){
	// RouteNode top = stack.top();
	// if(top == null){
	// return null;
	// }if(top.valid && top.left == baseNode){
	// return leftmostBaseNodeAndStack(top.right, stack);
	// }else{
	// stack.pop();
	// RouteNode prevTop = top;
	// top = stack.top();
	// while(top!= null && (!top.valid || top.right == prevTop)){
	// stack.pop();
	// prevTop = top;
	// top = stack.top();
	// }
	// if(top == null){
	// return null;
	// }else{
	// return leftmostBaseNodeAndStack(top.right, stack);
	// }
	// }
	// }

	@SuppressWarnings("unused")
	private boolean lessThanEqual(K key1, K key2) {
		if (comparator != null) {
			return comparator.compare(key1, key2) <= 0;
		} else {
			@SuppressWarnings("unchecked")
			Comparable<? super K> keyComp = (Comparable<? super K>) key1;
			return keyComp.compareTo(key2) <= 0;
		}
	}

	@SuppressWarnings("unchecked")
	final private ImmutableTreapMapHolder<K, V> getNextBaseNodeAndStack(Object baseNode, Stack<RouteNode> stack) {
		RouteNode top = stack.top();
		if (top == null) {
			return null;
		}
		if (top.left == baseNode) {
			return leftmostBaseNodeAndStack(top.right, stack);
		}
		K keyToBeGreaterThan = (K) top.key;
		while (top != null) {
			if (top.valid && lessThan(keyToBeGreaterThan, (K) top.key)) {
				return leftmostBaseNodeAndStack(top.right, stack);
			} else {
				stack.pop();
				top = stack.top();
			}
		}
		return null;
	}

	private boolean lessThan(K key1, K key2) {
		if (comparator != null) {
			return comparator.compare(key1, key2) < 0;
		} else {
			@SuppressWarnings("unchecked")
			Comparable<? super K> keyComp = (Comparable<? super K>) key1;
			return keyComp.compareTo(key2) < 0;
		}
	}



	private final class ThreadLocalBuffers {
		public Stack<RouteNode> getStack() {
			stack.resetStack();
			return stack;
		}

		long[] statistics = new long[33];

		public void increaseTraversedNodes() {
			statistics[16]++;
		}

		public void increaseRangeQueries() {
			statistics[17]++;
		}

		public long getTraversedNodes() {
			return statistics[16];
		}

		@SuppressWarnings("unused")
		public long getRangeQueries() {
			return statistics[17];
		}

		public Stack<RouteNode> getNextStack() {
			nextStack.resetStack();
			return nextStack;
		}

		public Stack<ImmutableTreapMapHolder<K, V>> getLockedBaseNodesStack() {
			lockedBaseNodesStack.resetStack();
			return lockedBaseNodesStack;
		}

		@SuppressWarnings("unused")
		public Stack<Object> getTraverseStack() {
			traverseStack.resetStack();
			return traverseStack;
		}

		public Stack<Object> getKeyReturnStack() {
			keyReturnStack.resetStack();
			return keyReturnStack;
		}

		@SuppressWarnings("unused")
		public Stack<ImmutableTreapMap.ImmutableTreapValue<K, V>> getReturnStack() {
			returnStack.resetStack();
			return returnStack;
		}

		@SuppressWarnings("unused")
		public LongStack getReadTokenStack() {
			readTokenStack.resetStack();
			return readTokenStack;
		}

		public Stack<Object> getReturnStack2() {
			returnStack2.resetStack();
			return returnStack2;
		}

		@SuppressWarnings("unused")
		public void setReturnStack2(Stack<Object> returnStack2) {
			this.returnStack2 = returnStack2;
		}

		public Stack<K> getOptimisticReturnStack() {
			optimisticReturnStack.resetStack();
			return optimisticReturnStack;
		}

		@SuppressWarnings("unused")
		public void setOptimisticReturnStack(Stack<K> optimisticReturnStack) {
			this.optimisticReturnStack = optimisticReturnStack;
		}

		private Stack<RouteNode> stack = new Stack<RouteNode>();
		private Stack<RouteNode> nextStack = new Stack<RouteNode>();
		private Stack<ImmutableTreapMapHolder<K, V>> lockedBaseNodesStack = new Stack<ImmutableTreapMapHolder<K, V>>();
		private Stack<ImmutableTreapMap.ImmutableTreapValue<K, V>> returnStack = new Stack<ImmutableTreapMap.ImmutableTreapValue<K, V>>(
				16);
		private Stack<Object> keyReturnStack = new Stack<Object>(16);

		private Stack<Object> traverseStack = new Stack<Object>();
		private Stack<Object> returnStack2 = new Stack<Object>(16);
		private Stack<K> optimisticReturnStack = new Stack<K>(16);

		private LongStack readTokenStack = new LongStack();
	}

	private ThreadLocal<ThreadLocalBuffers> threadLocalBuffers = new ThreadLocal<ThreadLocalBuffers>() {

		@Override
		protected ImmDataCATreeMap<K, V>.ThreadLocalBuffers initialValue() {
			return new ThreadLocalBuffers();
		}

	};

	public final void rangeUpdate(final K lo, final K hi, BiFunction<K, V, V> operation) {
		throw new RuntimeException("Not yet implemented");
	}




	private void unlockBaseNode(final LOCKING_MODE mode, ImmutableTreapMapHolder<K, V> baseNode) {
		if (mode == LOCKING_MODE.WRITE_LOCK)
			baseNode.unlock();
		else if (mode == LOCKING_MODE.READ_LOCK)
			baseNode.readUnlock();
	}

	private void lockBaseNode(final LOCKING_MODE mode, ImmutableTreapMapHolder<K, V> baseNode) {
		if (mode == LOCKING_MODE.WRITE_LOCK)
			baseNode.lock();
		else if (mode == LOCKING_MODE.READ_LOCK)
			baseNode.readLock();
	}


	@SuppressWarnings("unchecked")
	private final ImmutableTreapValue<K, V> optimisticSubSet(final K lo, final K hi) {
		// ThreadLocalBuffers tlbs = threadLocalBuffers.get();
		Stack<RouteNode> stack = new Stack<>();// .getStack();
		Stack<ImmutableTreapMapHolder<K, V>> lockedBaseNodesStack = new Stack<>();// tlbs.getLockedBaseNodesStack();
		LongStack readTokenStack = new LongStack();// tlbs.getReadTokenStack();
		ImmutableTreapMapHolder<K, V> baseNode;
		// Lock all base nodes that might contain keys in the range
		baseNode = getBaseNodeAndStack(lo, stack);
		long optimisticReadToken = baseNode.getOptimisticReadToken();
		if (!baseNode.isValid() || !baseNode.validateOptimisticReadToken(optimisticReadToken)) {
			return null; // Fail
		}
		// First base node successfully locked
		while (true) {
			// Add the successfully locked base node to the completed list
			lockedBaseNodesStack.push(baseNode);
			readTokenStack.push(optimisticReadToken);
			// Check if it is the end of our search
			K baseNodeMaxKey = baseNode.maxKey();
			if (baseNodeMaxKey != null && lessThan(hi, baseNodeMaxKey)) {
				break; // We have locked all base nodes that we need!
			}
			// There might be more base nodes in the range, continue
			baseNode = getNextBaseNodeAndStack(baseNode, stack);
			if (baseNode == null) {
				break;// The last base node is locked
			}
			optimisticReadToken = baseNode.getOptimisticReadToken();
			if (!baseNode.isValid() || !baseNode.validateOptimisticReadToken(optimisticReadToken)) {
				return null; // Fail
			}
		}
		// We have successfully locked all the base nodes that we need
		// Time to construct the results from the contents of the base nodes
		// The linearization point is just before the first lock is unlocked
		// Stack<STDAVLNode<K,V>> traverseStack = tlbs.getTraverseStack();
		// TreapStack returnStack = new TreapStack();//tlbs.getReturnStack();
		Object[] lockedBaseNodeArray = lockedBaseNodesStack.getStackArray();
		ImmutableTreapValue<K, V> root = ImmutableTreapMap.createEmpty();
		for (int i = 0; i < lockedBaseNodesStack.size(); i++) {
			ImmutableTreapMapHolder<K, V> map = (ImmutableTreapMapHolder<K, V>) (lockedBaseNodeArray[i]);
			root = ImmutableTreapMap.cheapJoin(root, map.getRoot());
			if (!map.validateOptimisticReadToken(optimisticReadToken)) {
				return null; // Fail
			}
			// returnStack.push(map.getRoot());
			// returnStack.getStackArray()[16] = map.getRoot();
			// if(ImmutableTreapMap.isEmpty(map.getRoot())){
			// System.out.println("HEJ");
			// };
			// ImmutableTreapMap.isEmpty(map.getRoot());
			// if (!map.validateOptimisticReadToken(optimisticReadToken)) {
			// return null; // Fail
			// }

			// traverseStack.resetStack();
		}
		for (int i = 0; i < lockedBaseNodesStack.size(); i++) {
			threadLocalBuffers.get().increaseTraversedNodes();
		}
		return root;// new TreapStack();//returnStack;
	}


	private static  enum LOCKING_MODE{
		READ_LOCK,
		WRITE_LOCK;
	};
	
	@SuppressWarnings("unchecked")
	private final Stack<ImmutableTreapMapHolder<K, V>> lockBaseNodes(final K lo, final K hi, final LOCKING_MODE mode) {
		ThreadLocalBuffers tlbs = threadLocalBuffers.get();
		Stack<RouteNode> stack = tlbs.getStack();
		Stack<RouteNode> nextStack = tlbs.getNextStack();
		Stack<ImmutableTreapMapHolder<K, V>> lockedBaseNodesStack = tlbs.getLockedBaseNodesStack();
		ImmutableTreapMapHolder<K, V> baseNode;
		boolean tryAgain;
		// Lock all base nodes that might contain keys in the range
		do {
			if(lo == null) {
				baseNode = getFirstBaseNodeAndStack(stack);
			} else {
				baseNode = getBaseNodeAndStack(lo, stack);
			}
			lockBaseNode(mode, baseNode);
			tryAgain = !baseNode.isValid();
			if (tryAgain) {
				unlockBaseNode(mode, baseNode);
				stack.resetStack();
			}
		} while (tryAgain);
		// First base node successfully locked
		outer: while (true) {
			// Add the successfully locked base node to the completed list
			lockedBaseNodesStack.push(baseNode);
			// Check if it is the end of our search
			K baseNodeMaxKey = baseNode.maxKey();
			if (hi != null && baseNodeMaxKey != null && lessThan(hi, baseNodeMaxKey)) {
				break; // We have locked all base nodes that we need!
			}
			// There might be more base nodes in the range, continue
			ImmutableTreapMapHolder<K, V> lastLockedBaseNode = baseNode;
			nextStack.copyStateFrom(stack); // Save the current position so we
											// can try again
			do {
				baseNode = getNextBaseNodeAndStack(lastLockedBaseNode, stack);
				if (baseNode == null) {
					break outer;// The last base node is locked
				}
				lockBaseNode(mode, baseNode);
				tryAgain = !baseNode.isValid();
				if (tryAgain) {
					unlockBaseNode(mode, baseNode);
					// Reset stack
					stack.copyStateFrom(nextStack);
				}
			} while (tryAgain);
		}
		return lockedBaseNodesStack;
	}
	
	private final Stack<ImmutableTreapMapHolder<K, V>> lockBaseNodes(final LOCKING_MODE mode) {
		return lockBaseNodes(null, null, mode);
	}
	
	@SuppressWarnings("unchecked")
	private final void unlockBaseNodes(Stack<ImmutableTreapMapHolder<K, V>> nodes, LOCKING_MODE mode) {
		Object[] lockedBaseNodeArray = nodes.getStackArray();
		if (mode == LOCKING_MODE.WRITE_LOCK && nodes.size() == 1) {
			threadLocalBuffers.get().increaseTraversedNodes();
			ImmutableTreapMapHolder<K, V> map = (ImmutableTreapMapHolder<K, V>) (lockedBaseNodeArray[0]);
			// map.addKeysInRangeToStack(lo, hi, consumer, traverseStack);
			// returnStack.push(map.getRoot());
			adaptIfNeeded((ImmutableTreapMapHolder<K, V>) (lockedBaseNodeArray[0]));
			unlockBaseNode(mode, map);
		} else if (mode == LOCKING_MODE.READ_LOCK && nodes.size() == 1) {
			threadLocalBuffers.get().increaseTraversedNodes();
			ImmutableTreapMapHolder<K, V> map = (ImmutableTreapMapHolder<K, V>) (lockedBaseNodeArray[0]);
			map.addToContentionStatistics();// Optimistic attempt failed
			// returnStack.push(map.getRoot());
			// map.addKeysInRangeToStack(lo, hi, consumer, traverseStack);
			unlockBaseNode(mode, map);
		} else {
			for (int i = 0; i < nodes.size(); i++) {
				threadLocalBuffers.get().increaseTraversedNodes();
				ImmutableTreapMapHolder<K, V> map = (ImmutableTreapMapHolder<K, V>) (lockedBaseNodeArray[i]);
				// returnStack.push(map.getRoot());
				map.subManyFromContentionStatistics();
				unlockBaseNode(mode, map);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private ImmutableTreapMapHolder<K, V> getFirstBaseNodeAndStack(Stack<RouteNode> stack) {
		Object currNode = root;
		while (currNode instanceof RouteNode) {
			RouteNode currNodeR = (RouteNode) currNode;
			stack.push(currNodeR);
			currNode = currNodeR.left;
		}
		return (ImmutableTreapMapHolder<K, V>) currNode;
	}

	@SuppressWarnings("unchecked")
	private final void subSet(final K lo, final K hi, LOCKING_MODE mode, Consumer<K> consumer) {
		ThreadLocalBuffers tlbs = threadLocalBuffers.get();
		Stack<RouteNode> stack = tlbs.getStack();
		Stack<RouteNode> nextStack = tlbs.getNextStack();
		Stack<ImmutableTreapMapHolder<K, V>> lockedBaseNodesStack = tlbs.getLockedBaseNodesStack();
		ImmutableTreapMapHolder<K, V> baseNode;
		boolean tryAgain;
		// Lock all base nodes that might contain keys in the range
		do {
			baseNode = getBaseNodeAndStack(lo, stack);
			lockBaseNode(mode, baseNode);
			tryAgain = !baseNode.isValid();
			if (tryAgain) {
				unlockBaseNode(mode, baseNode);
				stack.resetStack();
			}
		} while (tryAgain);
		// First base node successfully locked
		outer: while (true) {
			// Add the successfully locked base node to the completed list
			lockedBaseNodesStack.push(baseNode);
			// Check if it is the end of our search
			K baseNodeMaxKey = baseNode.maxKey();
			if (baseNodeMaxKey != null && lessThan(hi, baseNodeMaxKey)) {
				break; // We have locked all base nodes that we need!
			}
			// There might be more base nodes in the range, continue
			ImmutableTreapMapHolder<K, V> lastLockedBaseNode = baseNode;
			nextStack.copyStateFrom(stack); // Save the current position so we
											// can try again
			do {
				baseNode = getNextBaseNodeAndStack(lastLockedBaseNode, stack);
				if (baseNode == null) {
					break outer;// The last base node is locked
				}
				lockBaseNode(mode, baseNode);
				tryAgain = !baseNode.isValid();
				if (tryAgain) {
					unlockBaseNode(mode, baseNode);
					// Reset stack
					stack.copyStateFrom(nextStack);
				}
			} while (tryAgain);
		}
		// We have successfully locked all the base nodes that we need
		// Time to construct the results from the contents of the base nodes
		// The linearization point is just before the first lock is unlocked
		// Stack<ImmutableTreapValue<K, V>> returnStack = tlbs.getReturnStack();
		ImmutableTreapValue<K, V> root = null;
		Object[] lockedBaseNodeArray = lockedBaseNodesStack.getStackArray();
		if (mode == LOCKING_MODE.WRITE_LOCK && lockedBaseNodesStack.size() == 1) {
			threadLocalBuffers.get().increaseTraversedNodes();
			ImmutableTreapMapHolder<K, V> map = (ImmutableTreapMapHolder<K, V>) (lockedBaseNodeArray[0]);
			// map.addKeysInRangeToStack(lo, hi, consumer, traverseStack);
			// returnStack.push(map.getRoot());
			root = map.getRoot();
			adaptIfNeeded((ImmutableTreapMapHolder<K, V>) (lockedBaseNodeArray[0]));
			unlockBaseNode(mode, map);
		} else if (mode == LOCKING_MODE.READ_LOCK && lockedBaseNodesStack.size() == 1) {
			threadLocalBuffers.get().increaseTraversedNodes();
			ImmutableTreapMapHolder<K, V> map = (ImmutableTreapMapHolder<K, V>) (lockedBaseNodeArray[0]);
			map.addToContentionStatistics();// Optimistic attempt failed
			// returnStack.push(map.getRoot());
			root = map.getRoot();
			// map.addKeysInRangeToStack(lo, hi, consumer, traverseStack);
			unlockBaseNode(mode, map);
		} else {
			root = ImmutableTreapMap.createEmpty();
			for (int i = 0; i < lockedBaseNodesStack.size(); i++) {
				threadLocalBuffers.get().increaseTraversedNodes();
				ImmutableTreapMapHolder<K, V> map = (ImmutableTreapMapHolder<K, V>) (lockedBaseNodeArray[i]);
				// returnStack.push(map.getRoot());
				root = ImmutableTreapMap.cheapJoin(root, map.getRoot());
				map.subManyFromContentionStatistics();
				unlockBaseNode(mode, map);
			}
		}
		// Object[] returnStackArray = returnStack.getStackArray();
		// for (int i = 0; i < returnStack.size(); i++) {
		// ImmutableTreapValue<K, V> tree = (ImmutableTreapValue<K, V>)
		// returnStackArray[i];
		ImmutableTreapMap.traverseKeysInRange(root, lo, hi, consumer, comparator);
		// }
	}

	
	
	// ==== Functions for debuging and testing

	private int numberOfRouteNodes(Object currentNode) {
		if (currentNode == null) {
			return 0;
		} else {
			if (currentNode instanceof RouteNode) {
				RouteNode r = (RouteNode) currentNode;
				int sizeSoFar = numberOfRouteNodes(r.left);
				return 1 + sizeSoFar + numberOfRouteNodes(r.right);
			} else {
				return 0;
			}
		}
	}

	@SuppressWarnings("unused")
	private long getTraversedNodes() {
		return threadLocalBuffers.get().getTraversedNodes();
	}

	@SuppressWarnings("unused")
	private int numberOfRouteNodes() {
		// System.err.println("RANGE_QUERY_TRAVERSED_NODES " +
		// threadLocalBuffers.get().getTraversedNodes());
		// System.err.println("NUMBER OF RANGE QUERIES " +
		// threadLocalBuffers.get().getRangeQueries());
		// System.err.println("TRAVERSED NODE PER QUERY " +
		// ((double)threadLocalBuffers.get().getTraversedNodes())/((double)threadLocalBuffers.get().getRangeQueries()));
		return numberOfRouteNodes(root);
	}
	
	// ====== FOR DEBUGING ======
	@SuppressWarnings("unused")
	private final static boolean DEBUG = false;
	// ==========================
	
	private void printDotHelper(Object n, PrintStream writeTo, int level) {
		try {
			if (n instanceof RouteNode) {
				RouteNode node = (RouteNode) n;
				// LEFT
				writeTo.print("\"" + node + level + " \"");
				writeTo.print(" -> ");
				writeTo.print("\"" + node.left + (level + 1) + " \"");
				writeTo.println(";");
				// RIGHT
				writeTo.print("\"" + node + level + " \"");
				writeTo.print(" -> ");
				writeTo.print("\"" + node.right + (level + 1) + " \"");
				writeTo.println(";");

				printDotHelper(node.left, writeTo, level + 1);
				printDotHelper(node.right, writeTo, level + 1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void printDot(Object node, String fileName) {
		try {
			Stack<ImmutableTreapMapHolder<K, V>> nodes =lockBaseNodes(LOCKING_MODE.WRITE_LOCK);
			Process p = new ProcessBuilder("dot", "-Tpng")
					.redirectOutput(ProcessBuilder.Redirect.to(new File(fileName + ".png"))).start();
			PrintStream writeTo = new PrintStream(p.getOutputStream());
			writeTo.print("digraph G{\n");
			writeTo.print("  graph [ordering=\"out\"];\n");
			printDotHelper(node, writeTo, 0);
			writeTo.print("}\n");
			writeTo.close();
			p.waitFor();
			unlockBaseNodes(nodes, LOCKING_MODE.WRITE_LOCK);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	 @SuppressWarnings("unused")
	private void printDot(String fileName) {
		printDot(root, fileName);
	}

  	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		{
			System.out.println("Simple TEST");
			ImmDataCATreeMap<Integer, Integer> set = new ImmDataCATreeMap<Integer, Integer>();
			// Insert elements
			for (int i = 0; i < 100; i++) {
				set.put(i, 1000000);
			}
			// Test subSet
			Object[] array = set.subSet(0, 2);
			System.out.println("SUBSET SIZE = " + array.length);
			for (int i = 0; i < array.length; i++) {
				System.out.println(array[i]);
			}
			// Test get
			System.out.println("set.get(7) = " + set.get(7));
			System.out.println("Advanced TEST");
		}
		{
			ImmDataCATreeMap<Integer, Integer> set = new ImmDataCATreeMap<Integer, Integer>();
			// Insert elements
			for (int i = 0; i < 100; i++) {
				set.put(i, 1000000);
			}
			{
				ImmutableTreapMapHolder<Integer, Integer> baseNode = (ImmutableTreapMapHolder<Integer, Integer>) set
						.getBaseNode(50);
				baseNode.lock();
				set.highContentionSplit(baseNode);
				baseNode.unlock();
			}
			{
				ImmutableTreapMapHolder<Integer, Integer> baseNode = (ImmutableTreapMapHolder<Integer, Integer>) set
						.getBaseNode(25);
				baseNode.lock();
				set.highContentionSplit(baseNode);
				baseNode.unlock();
			}
			{
				ImmutableTreapMapHolder<Integer, Integer> baseNode = (ImmutableTreapMapHolder<Integer, Integer>) set
						.getBaseNode(75);
				baseNode.lock();
				set.highContentionSplit(baseNode);
				baseNode.unlock();
			}
			{
				ImmutableTreapMapHolder<Integer, Integer> baseNode = (ImmutableTreapMapHolder<Integer, Integer>) set
						.getBaseNode(1);
				baseNode.lock();
				set.highContentionSplit(baseNode);
				baseNode.unlock();
			}
			// Test subSet
			{
				Object[] array = set.subSet(-30, 50);
				System.out.println("SUBSET SIZE = " + array.length);
				for (int i = 0; i < array.length; i++) {
					System.out.println(array[i]);
				}
			}
			{
				Object[] array = set.subSet(10, 45);
				System.out.println("SUBSET SIZE = " + array.length);
				for (int i = 0; i < array.length; i++) {
					System.out.println(array[i]);
				}
			}
			{
				Object[] array = set.subSet(99, 105);
				System.out.println("SUBSET SIZE = " + array.length);
				for (int i = 0; i < array.length; i++) {
					System.out.println(array[i]);
				}
			}
			{
				Object[] array = set.subSet(-30, 130);
				System.out.println("SUBSET SIZE = " + array.length);
				for (int i = 0; i < array.length; i++) {
					System.out.println(array[i]);
				}
			}
			{
				Object[] array = set.subSet(50, 50);
				System.out.println("SUBSET SIZE = " + array.length);
				for (int i = 0; i < array.length; i++) {
					System.out.println(array[i]);
				}
			}
			{
				Object[] array = set.subSet(12, 34);
				System.out.println("SUBSET SIZE = " + array.length);
				for (int i = 0; i < array.length; i++) {
					System.out.println(array[i]);
				}
			}
			// Test get
			System.out.println("set.get(7) = " + set.get(7));
		}
	}
	// === End of debug functions ==================


	// === Constructors ============================

	public ImmDataCATreeMap() {
		comparator = null;
	}

	public ImmDataCATreeMap(Comparator<? super K> comparator) {
		this.comparator = comparator;
	}


	//TODO rewrite this to make use of snapshot functionality
	public int size() {
		Stack<ImmutableTreapMapHolder<K, V>> nodes = lockBaseNodes(LOCKING_MODE.READ_LOCK);
		int size = sizeHelper(root);
		unlockBaseNodes(nodes, LOCKING_MODE.READ_LOCK);
		return size;
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public boolean containsKey(Object key) {
		threadLocalBuffers.get().getReturnStack2();
		threadLocalBuffers.get().getOptimisticReturnStack();
		return get(key) != null;
	}

	@SuppressWarnings("unchecked")
	public V get(Object key) {
		while (true) {
			ImmutableTreapMapHolder<K, V> baseNode = (ImmutableTreapMapHolder<K, V>) getBaseNode(key);
			// First do an optimistic attempt
			long optimisticReadToken = baseNode.getOptimisticReadToken();
			if (0L != optimisticReadToken && baseNode.isValid()) {
				ImmutableTreapValue<K, V> root = baseNode.getRoot();
				if (baseNode.validateOptimisticReadToken(optimisticReadToken)) {
					return ImmutableTreapMap.get(root, (K) key, comparator);
				}
			}
			// Optimistic attempt failed, do the normal approach
			baseNode.readLock();
			baseNode.addToContentionStatistics();// Because the optimistic
												 // attempt failed
			// Check if valid
			if (baseNode.isValid() == false) {
				baseNode.readUnlock();
				continue; // retry
			}
			// Do the operation
			ImmutableTreapValue<K, V> root = baseNode.getRoot();
			baseNode.readUnlock();
			return ImmutableTreapMap.get(root, (K) key, comparator);
		}
	}

	public V put(K key, V value) {
		while (true) {
			@SuppressWarnings("unchecked")
			ImmutableTreapMapHolder<K, V> baseNode = (ImmutableTreapMapHolder<K, V>) getBaseNode(key);
			baseNode.lock();
			// Check if valid
			if (!baseNode.isValid()) {
				baseNode.unlock();
				continue; // retry
			}
			// Do the operation
			V result = baseNode.put(key, value);
			adaptIfNeeded(baseNode);
			baseNode.unlock();
			return result;
		}
	}

	public V putIfAbsent(K key, V value) {
		while (true) {
			@SuppressWarnings("unchecked")
			ImmutableTreapMapHolder<K, V> baseNode = (ImmutableTreapMapHolder<K, V>) getBaseNode(key);
			baseNode.lock();
			// Check if valid
			if (!baseNode.isValid()) {
				baseNode.unlock();
				continue; // retry
			}
			// Do the operation
			V result = baseNode.putIfAbsent(key, value);
			adaptIfNeeded(baseNode);
			baseNode.unlock();
			return result;
		}
	}

	public V remove(Object key) {
		while (true) {
			@SuppressWarnings("unchecked")
			ImmutableTreapMapHolder<K, V> baseNode = (ImmutableTreapMapHolder<K, V>) getBaseNode(key);
			baseNode.lock();
			// Check if valid
			if (baseNode.isValid() == false) {
				baseNode.unlock();
				continue; // retry
			}
			// Do the operation
			V result = baseNode.remove(key);
			adaptIfNeeded(baseNode);
			baseNode.unlock();
			return result;
		}
	}

	public void clear() {
		Stack<ImmutableTreapMapHolder<K, V>> nodes =lockBaseNodes(LOCKING_MODE.WRITE_LOCK);
		root = new ImmutableTreapMapHolder<K, V>();
		Object[] ns = nodes.getStackArray();
		for(int i = 0; i < nodes.size(); i++ ) {
			@SuppressWarnings("unchecked")
			ImmutableTreapMapHolder<K, V> node = (ImmutableTreapMapHolder<K, V>) ns[i];
			node.invalidate();
		}
		unlockBaseNodes(nodes, LOCKING_MODE.WRITE_LOCK);
	}
	
	// Set<K> keySet();
	// Collection<V> values();
	// TODO this should preserve order make use of snapshot capability 
	public Set<Map.Entry<K, V>> entrySet() {
		LinkedList<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>();
		Stack<ImmutableTreapMapHolder<K, V>> nodes =lockBaseNodes(LOCKING_MODE.READ_LOCK);
		addAllToList(root, list);
		unlockBaseNodes(nodes, LOCKING_MODE.READ_LOCK);
		return new HashSet<Map.Entry<K, V>>(list);
	}
	
	// 0 = write lock directly
	// 1 = read lock directly

	public final Object[] subSet(final K lo, final K hi) {
		Stack<Object> returnStack = threadLocalBuffers.get().getKeyReturnStack();
		subSet(lo, hi, (k) -> returnStack.push(k));
		int returnSize = returnStack.size();
		Object[] returnArray = new Object[returnSize];
		Object[] returnStackArray = returnStack.getStackArray();
		for (int i = 0; i < returnSize; i++) {
			returnArray[i] = returnStackArray[i];
		}
		return returnArray;

	}

	public void subSet(final K lo, final K hi, Consumer<K> consumer) {
		// TreapStack returnValue = null;
		// returnValue =
		threadLocalBuffers.get().increaseRangeQueries();
		ImmutableTreapValue<K, V> returnValue = optimisticSubSet(lo, hi);
		if (null == returnValue) {
			// System.out.print("F");
			subSet(lo, hi, LOCKING_MODE.READ_LOCK, consumer);
		} else {
			ImmutableTreapMap.traverseKeysInRange(returnValue, lo, hi, consumer, comparator);
		}
	}
	

	
	@Override
	public boolean remove(Object key, Object value) {
		// TODO Auto-generated method stub
	return false;
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public V replace(K key, V value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entry<K, V> lowerEntry(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public K lowerKey(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entry<K, V> floorEntry(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public K floorKey(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entry<K, V> ceilingEntry(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public K ceilingKey(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entry<K, V> higherEntry(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public K higherKey(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entry<K, V> firstEntry() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entry<K, V> lastEntry() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entry<K, V> pollFirstEntry() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entry<K, V> pollLastEntry() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Comparator<? super K> comparator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public K firstKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public K lastKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> descendingMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NavigableSet<K> keySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		// TODO Auto-generated method stub
		return null;
	}
  
}
