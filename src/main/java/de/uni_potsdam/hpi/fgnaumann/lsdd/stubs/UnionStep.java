package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Reducer that unions the set of duplicate pairs by emiting only one pair
 * for each reducer
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class UnionStep extends ReduceStub {
	@Override
	public void reduce(Iterator<PactRecord> records,
			Collector<PactRecord> out) throws Exception {
		if (records.hasNext()) {
			PactRecord record = records.next().createCopy();
			out.collect(record);
		}
	}
}