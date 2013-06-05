package de.uni_potsdam.hpi.fgnaumann.lsdd;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
	 * Reducer that expands the blocking keys of the records in unbalanced
	 * blocks
	 * 
	 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
	 * 
	 */
	public class SecondBlockingStep extends ReduceStub {
		@Override
		public void reduce(Iterator<PactRecord> records,
				Collector<PactRecord> out) throws Exception {
			while (records.hasNext()) {
				PactRecord recordToExpand = records.next();
				PactString appliedBlockingFunctionId = recordToExpand.getField(
						MultiBlocking.BLOCKING_ID_FIELD, PactString.class);
				for (BlockingFunction bf : BlockingFunction.blockingFuntions) {
					if (appliedBlockingFunctionId.equals(bf.getID())) {
						out.collect(bf
								.copyWithExplodedBlockingKey(recordToExpand));
						break;
					}
				}
			}
		}
	}