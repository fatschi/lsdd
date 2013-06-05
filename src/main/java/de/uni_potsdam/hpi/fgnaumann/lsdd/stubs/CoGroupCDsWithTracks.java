package de.uni_potsdam.hpi.fgnaumann.lsdd.stubs;

import java.util.Iterator;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import de.uni_potsdam.hpi.fgnaumann.lsdd.TrackList;

import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactList;

/**
 * CoGrouper that combines CD's with tracks
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class CoGroupCDsWithTracks extends CoGroupStub {

	@Override
	public void coGroup(Iterator<PactRecord> inputRecords,
			Iterator<PactRecord> concatRecords, Collector<PactRecord> out) {
		while (inputRecords.hasNext()) {
			PactRecord outputRecord = inputRecords.next().createCopy();
			PactList<PactRecord> trackList = new TrackList();
			while (concatRecords.hasNext()) {
				trackList.add(concatRecords.next().createCopy());
			}
			outputRecord.setField(MultiBlocking.TRACKS_FIELD, trackList);
			out.collect(outputRecord);
		}

	}
}