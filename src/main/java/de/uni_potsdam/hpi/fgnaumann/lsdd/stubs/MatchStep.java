package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import de.uni_potsdam.hpi.fgnaumann.lsdd.similarity.SimilarityMeasure;
import de.uni_potsdam.hpi.fgnaumann.lsdd.util.DuplciateEmitter;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Reducer that does the matching step for each record in the block
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * @author richard.meissner@student.hpi.uni-potsdam.de
 * 
 */
public class MatchStep extends ReduceStub {
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		PactRecord tempRecord = new PactRecord();
		List<PactRecord> r_temp = new ArrayList<PactRecord>();
		while (records.hasNext()) {
			tempRecord = records.next();
			r_temp.add(tempRecord.createCopy());
		}
		for (int i = 0; i < r_temp.size(); i++) {
			for (int j = i + 1; j < r_temp.size(); j++) {
				PactRecord r1 = r_temp.get(i);
				PactRecord r2 = r_temp.get(j);

				if (SimilarityMeasure.isDuplicate(r1, r2)) {
					DuplciateEmitter de = new DuplciateEmitter(out, 0);
					de.emitDuplicate(r1, r2);
				}
			}
		}
	}
}