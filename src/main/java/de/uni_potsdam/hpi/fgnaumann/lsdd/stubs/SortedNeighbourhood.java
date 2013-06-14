package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import de.uni_potsdam.hpi.fgnaumann.lsdd.similarity.SimilarityMeasure;
import de.uni_potsdam.hpi.fgnaumann.lsdd.util.DuplciateEmitter;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Reducer that expands the blocking keys of the records in unbalanced blocks
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class SortedNeighbourhood extends ReduceStub {
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		//store only the current window in a queue
		Queue<PactRecord> recordsQueue = new LinkedList<PactRecord>();
		// get the first record to calculate the window size based on the block size
		PactRecord tempRecord = records.next();

		int blockSize = tempRecord.getField(MultiBlocking.COUNT_FIELD,
				PactInteger.class).getValue();
		int windowSize = MultiBlocking.MAX_WINDOW_SIZE < MultiBlocking.MAXIMUM_COMPARISON
				/ blockSize ? MultiBlocking.MAX_WINDOW_SIZE
				: MultiBlocking.MAXIMUM_COMPARISON / blockSize;
		
		//initially store the first #windowSize records in the queue 
		recordsQueue.offer(tempRecord.createCopy());
		while (records.hasNext() && recordsQueue.size() < windowSize) {
			if (!recordsQueue.offer(records.next().createCopy()))
				break;
		}
		
		PactRecord record = recordsQueue.poll();
		while (record != null) {
			for (PactRecord recordToCompare : recordsQueue) {
				if (SimilarityMeasure.isDuplicate(record, recordToCompare)) {
					DuplciateEmitter de = new DuplciateEmitter(out);
					de.emitDuplicate(record, recordToCompare);
				}
			}
			record = recordsQueue.poll();
			if (records.hasNext()) {
				recordsQueue.offer(records.next().createCopy());
			}
		}
	}
}