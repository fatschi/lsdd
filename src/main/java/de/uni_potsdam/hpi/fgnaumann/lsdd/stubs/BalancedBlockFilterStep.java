package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import de.uni_potsdam.hpi.fgnaumann.lsdd.BlockingFunction;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Mapper that emits only unbalanced blocks
 * 
 * @author richard.meissner@student.hpi.uni-potsdam.de
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class BalancedBlockFilterStep extends MapStub {

	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {
		if (record.getField(MultiBlocking.COUNT_FIELD, PactInteger.class)
				.getValue() > MultiBlocking.THRESHOLD) {
			PactString appliedBlockingFunctionId = record.getField(
					MultiBlocking.BLOCKING_ID_FIELD, PactString.class);
			for (BlockingFunction bf : BlockingFunction.blockingFuntions) {
				if (appliedBlockingFunctionId.equals(bf.getID())) {
					collector.collect(bf.setExplodedBlockingKey(record));
					break;
				}
			}
		}
	}
}