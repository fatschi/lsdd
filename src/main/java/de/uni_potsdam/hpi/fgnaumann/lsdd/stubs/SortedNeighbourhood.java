package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import de.uni_potsdam.hpi.fgnaumann.lsdd.BlockingFunction;
import de.uni_potsdam.hpi.fgnaumann.lsdd.BlockingKeyComparator;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import de.uni_potsdam.hpi.fgnaumann.lsdd.SimilarityMeasure;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
	 * Reducer that expands the blocking keys of the records in unbalanced
	 * blocks
	 * 
	 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
	 * 
	 */
	public class SortedNeighbourhood extends ReduceStub {
		@Override
		public void reduce(Iterator<PactRecord> records,
				Collector<PactRecord> out) throws Exception {
			List<PactRecord> records_list = new ArrayList<PactRecord>();
			while (records.hasNext()) {
				PactRecord recordToExpand = records.next();
				PactString appliedBlockingFunctionId = recordToExpand.getField(
						MultiBlocking.BLOCKING_ID_FIELD, PactString.class);
				for (BlockingFunction bf : BlockingFunction.blockingFuntions) {
					if (appliedBlockingFunctionId.equals(bf.getID())) {
						records_list.add(bf
								.copyWithExplodedBlockingKey(recordToExpand));
						break;
					}
				}
			}
			
			Collections.sort(records_list, new BlockingKeyComparator());
			
			for (int i = 0; i < records_list.size(); i++) {
				for (int j = i + 1; j < j+MultiBlocking.WINDOW_SIZE && j < records_list.size(); j++) {
					PactRecord r1 = records_list.get(i);
					PactRecord r2 = records_list.get(j);
					
					if(SimilarityMeasure.isDuplicate(r1, r2)){
						PactRecord outputRecord = new PactRecord();
						if (r1.getField(0, PactInteger.class).getValue() < r2
								.getField(0, PactInteger.class).getValue()) {
							outputRecord.setField(MultiBlocking.DUPLICATE_ID_1_FIELD,
									r1.getField(0, PactInteger.class));
							outputRecord.setField(MultiBlocking.DUPLICATE_ID_2_FIELD,
									r2.getField(0, PactInteger.class));
						} else {
							outputRecord.setField(MultiBlocking.DUPLICATE_ID_2_FIELD,
									r1.getField(0, PactInteger.class));
							outputRecord.setField(MultiBlocking.DUPLICATE_ID_1_FIELD,
									r2.getField(0, PactInteger.class));
						}
						out.collect(outputRecord);
					}
				}
			}
		}
	}