package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Reducer that counts the entries of each block to identify unbalanced
 * blocks
 * 
 * @author richard.meissner@student.hpi.uni-potsdam.de
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class CountStep extends ReduceStub {
	@Override
	public void reduce(Iterator<PactRecord> records,
			Collector<PactRecord> out) throws Exception {
		PactRecord outputRecord;
		List<PactRecord> r_temp = new ArrayList<PactRecord>();
		int sum = 0;
		while (records.hasNext()) {
			outputRecord = records.next();
			r_temp.add(outputRecord.createCopy());
			sum++;
		}
		PactInteger cnt = new PactInteger();
		cnt.setValue(sum);
		for (PactRecord r : r_temp) {
			r.setField(MultiBlocking.COUNT_FIELD, cnt);
			out.collect(r);
		}
	}
}