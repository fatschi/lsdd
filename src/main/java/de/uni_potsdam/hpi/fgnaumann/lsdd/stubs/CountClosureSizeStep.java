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
 * Reducer that counts the entries of each block to identify unbalanced blocks
 * 
 * @author richard.meissner@student.hpi.uni-potsdam.de
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class CountClosureSizeStep extends ReduceStub {
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		PactRecord record = records.next();
		PactRecord outputRecord = new PactRecord();
		outputRecord.setField(0, record.getField(
				MultiBlocking.DUPLICATE_REDUCE_FIELD, PactInteger.class));

		List<PactRecord> r_temp = new ArrayList<PactRecord>();
		int sum = 1;
		while (records.hasNext()) {
			r_temp.add(records.next().createCopy());
			sum++;
		}
		outputRecord.setField(1, new PactInteger(sum));

		out.collect(outputRecord);
	}
}