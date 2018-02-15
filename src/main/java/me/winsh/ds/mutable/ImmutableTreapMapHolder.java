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

package me.winsh.ds.mutable;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import me.winsh.ds.immutable.ImmutableTreapMap;
import me.winsh.ds.immutable.ImmutableTreapMap.ImmutableTreapValue;
import me.winsh.ds.mutable.interfaces.AnyKeyProviding;
import me.winsh.ds.mutable.interfaces.Invalidatable;
import me.winsh.ds.mutable.interfaces.SplitableAndJoinableMap;

public class ImmutableTreapMapHolder<K,V> extends AbstractMap<K, V> implements SplitableAndJoinableMap<K, V>, Invalidatable, AnyKeyProviding<K> {

	
	//STUFF NEEDED FOR INTEGRATION WITH CA TREE
	
    private final StampedLock sleepingLock;
    private final Lock sleepingWLock;//= lock.asWriteLock();
    private final Lock sleepingRLock;//= lock.asReadLock();
    private int statLockStatistics = 0;
    private boolean valid = true;
    private K maxKey = null;
    //Use setRoot and getRoot to access the root
    private Object parent = null;
    private static final int STAT_LOCK_HIGH_CONTENTION_LIMIT = 1000;
    private static final int STAT_LOCK_LOW_CONTENTION_LIMIT = -1000;
    private static final int STAT_LOCK_FAILURE_CONTRIB = 250;
    private static final int STAT_LOCK_SUCCESS_CONTRIB = 1;
	
	
	
    public void setParent(Object parent){
        this.parent = parent;
    }

    public Object getParent(){
        return parent;
    }

    //=== Public functions and helper functions ===

    //=== Any key providing functions =============

    public K anyKey(){
        return ImmutableTreapMap.minKey(root);
    }

    //=== Invalidatable functions =================

    public boolean isValid(){
        return valid;
    }

    public void invalidate(){
        valid = false;
    }


    //=== Lock Functions ==========================

    public boolean tryLock(){

    		return sleepingWLock.tryLock();

    }
    
    public void lock(){
        if (tryLock()) {
            subFromContentionStatistics();
            return;
        }
        lockNoStats();
        addToContentionStatistics();
    }
    
	public boolean lockIsContended() {
        if (tryLock()) {
            return false;
        }
        lockNoStats();
        return true;
	}
    
	public void lockNoStats() {
        	sleepingWLock.lock();

	}

    public void addToContentionStatistics(){
    		statLockStatistics += STAT_LOCK_FAILURE_CONTRIB;
    }

    public void subFromContentionStatistics(){
   			statLockStatistics -= STAT_LOCK_SUCCESS_CONTRIB;
    }
    
    public void subManyFromContentionStatistics(){
   			statLockStatistics -= 100;
    }

    public void unlock(){
   			sleepingWLock.unlock();
    }

    public void readLock(){
   			sleepingRLock.lock();

    }

    public void readUnlock(){
   			sleepingRLock.unlock();
    }


    public long getOptimisticReadToken(){
   			return sleepingLock.tryOptimisticRead();
    }

    public boolean validateOptimisticReadToken(long optimisticReadToken){
   			return sleepingLock.validate(optimisticReadToken);
    }

    public int getStatistics(){
   			return statLockStatistics;
    }
    
    public void resetStatistics(){
   			statLockStatistics = 0;
    }
    
    public int getHighContentionLimit(){
        return STAT_LOCK_HIGH_CONTENTION_LIMIT;
    }

    public int getLowContentionLimit(){
        return STAT_LOCK_LOW_CONTENTION_LIMIT;
    }

    public boolean isHighContentionLimitReached(){
   			return statLockStatistics > STAT_LOCK_HIGH_CONTENTION_LIMIT;
    }
    
    public boolean isLowContentionLimitReached(){
   			return statLockStatistics < STAT_LOCK_LOW_CONTENTION_LIMIT;
    }
	
    public final void addAllToList(LinkedList<Map.Entry<K, V>> list){
		ImmutableTreapMap.traverseAllItems(root, (k,v) -> list.add(
				new AbstractMap.SimpleImmutableEntry<K,V>(k, v){


					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public int hashCode(){
	                    return k.hashCode();
	                }
	            }
				));
    } 
	
	//END =======================================


    private int compare(K key1, K key2){
    	if(comparator == null){
    		@SuppressWarnings("unchecked")
            Comparable<? super K> keyComp = (Comparable<? super K>) key1;
            return keyComp.compareTo(key2);
    	}else{
    		return comparator.compare(key1, key2);
    	}
    }
	
	//Number of elements per node (should be 3 or greater)
    private volatile ImmutableTreapValue<K, V> root = ImmutableTreapMap.createEmpty();
	private Comparator<? super K> comparator = null;
	public ImmutableTreapMapHolder() {
        this(null);
    }

    public ImmutableTreapMapHolder(Comparator<? super K> comparator) {
        this.comparator  = comparator;
        root = ImmutableTreapMap.createEmpty();
        	sleepingLock = new StampedLock();
        	sleepingWLock = sleepingLock.asWriteLock();//= lock.asWriteLock();
            sleepingRLock = sleepingLock.asReadLock();
    }
	
		
	@Override
	public int size() {
		return computeActualSize() ;
	}

	@Override
	public boolean isEmpty() {
		return ImmutableTreapMap.isEmpty(root);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean containsKey(Object key) {
		return ImmutableTreapMap.get(root, (K)key, comparator) != null;
	}


	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		return ImmutableTreapMap.get(root, (K)key, comparator);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V put(K key, V value) {
	        if(maxKey == null || compare(key, maxKey) > 0){
		   maxKey = key;
	        }
		root = ImmutableTreapMap.put(root, key, value, comparator);
		return (V) ImmutableTreapMap.getPrevValue();
	}
	@SuppressWarnings("unchecked")
	public V putIfAbsent(K key, V value) {
	        if(maxKey == null || compare(key, maxKey) > 0){
		   maxKey = key;
	        }
	        root = ImmutableTreapMap.putIfAbsent(root, key, value, comparator);
		return (V) ImmutableTreapMap.getPrevValue();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
	        if(maxKey != null && compare((K)key, maxKey) == 0){
		   maxKey = ImmutableTreapMap.maxKey(root);
	        }
		root = ImmutableTreapMap.remove(root, (K)key, comparator);
		return (V) ImmutableTreapMap.getPrevValue();
	}

	@Override
	public void clear() {
        root = ImmutableTreapMap.createEmpty();
	}

	@Override
	public SplitableAndJoinableMap<K, V> join(
			SplitableAndJoinableMap<K, V> right) {
		ImmutableTreapMapHolder<K, V> newMap;
		if(comparator == null){
			newMap = new ImmutableTreapMapHolder<K, V>();
		}else{
			newMap = new ImmutableTreapMapHolder<K, V>(comparator);
		}
		newMap.root = ImmutableTreapMap.join(this.root, ((ImmutableTreapMapHolder<K, V>)right).root); 
		newMap.maxKey = ((ImmutableTreapMapHolder<K, V>)right).maxKey;
		return newMap;
	}

	@Override
	public SplitableAndJoinableMap<K, V> split(Object[] splitKeyWriteBack,
			SplitableAndJoinableMap<K, V>[] rightTreeWriteBack) {
		ImmutableTreapMapHolder<K, V> newLeftPart;
		ImmutableTreapMapHolder<K, V> newRightPart;
		if (comparator == null) {
			newLeftPart = new ImmutableTreapMapHolder<K, V>();
			newRightPart = new ImmutableTreapMapHolder<K, V>();
		} else {
			newLeftPart = new ImmutableTreapMapHolder<K, V>(comparator);
			newRightPart = new ImmutableTreapMapHolder<K, V>(comparator);
		}
		newLeftPart.root = ImmutableTreapMap.splitLeft(root);
		newRightPart.root = ImmutableTreapMap.splitRight(root);
		newLeftPart.maxKey = ImmutableTreapMap.maxKey(newLeftPart.root);
		newRightPart.maxKey = ImmutableTreapMap.maxKey(newRightPart.root);
		splitKeyWriteBack[0] = ImmutableTreapMap.minKey(newRightPart.root);
		rightTreeWriteBack[0] = newRightPart;
		return newLeftPart;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		TreeMap<K, V> entrySet;
		if(comparator == null){
			entrySet = new TreeMap<K, V>();
		}else{
			entrySet = new TreeMap<K, V>(comparator);
		}
		ImmutableTreapMap.traverseAllItems(root, (k,v) -> entrySet.put(k, v));
		return entrySet.entrySet();
	}
	
	public static void main(String[] args){
		ImmutableTreapMapHolder<Integer, Integer> map= new ImmutableTreapMapHolder<Integer, Integer>();
		for(int i = 10; i >= 0; i--){
			System.out.println("INSERT: " + i);
			map.put(i, i);
		}
		for(Integer e: map.keySet()){
			System.out.println(e);
		}
		for(int i = 10; i >= 0; i--){
			System.out.println("GET: " + i);
			System.out.println(map.get(i));
		}
		for(int i = 10; i >= 0; i--){
			System.out.println("Remove: " + i);
			map.remove(i, i);
		}
//		for(Integer e: map.keySet()){
//			System.out.println(e);
//		}
		for(int i = 0; i < 100000; i++){
			//System.out.println("INSERT: " + i);
			map.put(i, i);
		}
		System.out.println(map.get(50000));
	}

	public boolean hasLessThanTwoElements() {
		return ImmutableTreapMap.lessThanTwoElements(root);
	}

	public K maxKey() {
	    return maxKey;
	    //return ImmutableTreapMap.maxKey(root);
	}
	
	public void traverseKeysInRange(K lo, K hi, Consumer<K> consumer) {
	    ImmutableTreapMap.traverseKeysInRange(root, lo, hi, consumer, comparator);
	}

	public void performOperationToValuesInRange(K lo, K hi, BiFunction<K,V,V> operation) {
		throw new RuntimeException("Not yet defined");
	}

	private final class Counter{
		public int count = 0;
	}
	
	private int computeActualSize() {
		final Counter c = new Counter();
		ImmutableTreapMap.traverseAllItems(root, (k,v) ->c.count++);
		return c.count;
	}

	public ImmutableTreapValue<K, V> getRoot() {
		return root;
	}




}
