package de.uni_potsdam.hpi.fgnaumann.lsdd;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Mapper that applies the blocking functions to each record
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class FirstBlockingStep extends MapStub {

	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {
		for (BlockingFunction bf : BlockingFunction.blockingFuntions) {
			collector.collect(bf.copyWithBlockingKey(record));
		}
	}

}