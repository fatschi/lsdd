package de.uni_potsdam.hpi.fgnaumann.lsdd;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Mapper that emits only balanced blocks
 * 
 * @author richard.meissner@student.hpi.uni-potsdam.de
 * 
 */
public class UnbalancedBlockFilterStep extends MapStub {

	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {
		if (record.getField(MultiBlocking.COUNT_FIELD, PactInteger.class).getValue() <= MultiBlocking.THRESHOLD)
			collector.collect(record);
	}
}