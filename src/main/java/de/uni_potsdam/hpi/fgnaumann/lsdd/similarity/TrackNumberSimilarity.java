package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class TrackNumberSimilarity implements PositiveRule {
	private static TrackNumberSimilarity instance = null;

	private TrackNumberSimilarity() {
	}

	public static TrackNumberSimilarity getInstance() {
		if (instance == null) {
			instance = new TrackNumberSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		int record1DiscTracks = record1.getField(MultiBlocking.DISC_TRACKS_FIELD, PactInteger.class).getValue();
		int record2DiscTracks = record2.getField(MultiBlocking.DISC_TRACKS_FIELD, PactInteger.class).getValue();
		return 1f/(Math.abs(record2DiscTracks-record1DiscTracks)+1);
		
	}

	@Override
	public int getWeight() {
		return 3;
	}
}
