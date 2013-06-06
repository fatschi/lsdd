package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import java.util.Iterator;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Reducer that counts the entries of each block to identify unbalanced blocks
 * 
 * @author richard.meissner@student.hpi.uni-potsdam.de
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class CountOutputStep extends ReduceStub {
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		PactRecord outputRecord = new PactRecord();
		outputRecord.setField(
				0,
				records.next().getField(MultiBlocking.COUNT_FIELD,
						PactInteger.class));
		outputRecord.setField(
				1,
				records.next().getField(MultiBlocking.BLOCKING_ID_FIELD,
						PactString.class));
		outputRecord.setField(
				1,
				records.next().getField(MultiBlocking.BLOCKING_KEY_FIELD,
						PactString.class));
		out.collect(outputRecord);
	}
}