package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * @autor Jens Hildebrandt
 * @author Fabian Tschirschnitz
 *
 */

public class TransitiveClosureStep extends ReduceStub {
	private Map<Integer, SortedSet<Integer>> duplicates = new HashMap<Integer, SortedSet<Integer>>();
	private PactRecord outPut= new PactRecord();
	@Override
	public void reduce(Iterator<PactRecord> in, Collector<PactRecord> out)
			throws Exception {
		PactRecord element = null;
		while (in.hasNext()) {
			element = in.next();
			Integer firstID = element.getField(MultiBlocking.DUPLICATE_ID_1_FIELD, PactInteger.class).getValue();
			Integer secondID = element.getField(MultiBlocking.DUPLICATE_ID_2_FIELD, PactInteger.class).getValue();
			this.insertDuplicates(firstID, secondID);
		}
		
		Iterator<Integer> iterator= this.duplicates.keySet().iterator();
		
		while (iterator.hasNext()) {
			Integer key = iterator.next();
			outPutTransitiveClosureForTuple(key, out);
		}
	}
	
	public void outPutTransitiveClosureForTuple(Integer key, Collector<PactRecord> out) {
		SortedSet<Integer> reprSet = this.duplicates.get(this.duplicates.get(key).first());
		PactInteger repr = new PactInteger(reprSet.first());
		PactInteger closureSize = new PactInteger(reprSet.size());
		for(Integer value : reprSet.tailSet(key+1)) {
			this.outPut.setField(MultiBlocking.DUPLICATE_ID_1_FIELD, new PactInteger(key));
			this.outPut.setField(MultiBlocking.DUPLICATE_ID_2_FIELD, new PactInteger(value));
			this.outPut.setField(MultiBlocking.DUPLICATE_REDUCE1_FIELD, closureSize);
			this.outPut.setField(MultiBlocking.DUPLICATE_REDUCE2_FIELD, repr);
			out.collect(this.outPut.createCopy());
		}
	}
	
	public void insertDuplicates(Integer first, Integer second) {
		if (this.duplicates.containsKey(first)) {
			if (this.duplicates.containsKey(second)) {
				SortedSet<Integer> firstList = this.duplicates.get(first); 
				SortedSet<Integer> secondList = this.duplicates.get(second);
				//merge sets
				firstList.addAll(secondList);
						
				firstList.add(second);
				firstList.add(first);

				this.duplicates.put(first, firstList);
				this.duplicates.put(second, firstList);
			} else {
				SortedSet<Integer> firstList = this.duplicates.get(first);
				firstList.add(second);
				this.duplicates.put(first, firstList);
				this.duplicates.put(second, firstList);
			}
		} else {
			if (this.duplicates.containsKey(second)) {
				SortedSet<Integer> secondList = this.duplicates.get(second);
				secondList.add(first);
				this.duplicates.put(first, secondList);
				this.duplicates.put(second, secondList);
			} else {
				SortedSet<Integer> list = new TreeSet<Integer>();
				list.add(first);
				list.add(second);
				this.duplicates.put(first, list);
				this.duplicates.put(second, list);
			}
		}
		
	}

}