package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import de.uni_potsdam.hpi.fgnaumann.lsdd.BlockingFunction;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import de.uni_potsdam.hpi.fgnaumann.lsdd.util.UnicodeUtils;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Mapper that applies the blocking functions to each record
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class FirstBlockingStep extends MapStub {

	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {
		normalizeRecord(record);
		for (BlockingFunction bf : BlockingFunction.blockingFuntions) {
			collector.collect(bf.copyWithBlockingKey(record));
		}
	}

	private void normalizeRecord(PactRecord record) {
		UnicodeUtils.normalizeUnicode(record.getField(MultiBlocking.ARTIST_NAME_FIELD, PactString.class));
		UnicodeUtils.normalizeUnicode(record.getField(MultiBlocking.DISC_TITLE_FIELD, PactString.class));
	}

}