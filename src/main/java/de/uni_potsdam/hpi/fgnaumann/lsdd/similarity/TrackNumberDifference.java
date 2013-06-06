package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import de.uni_potsdam.hpi.fgnaumann.lsdd.SimilarityMeasure;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class TrackNumberDifference implements NegativeRule{
    private static TrackNumberDifference instance = null;
 
    private TrackNumberDifference() {}
 
    public static TrackNumberDifference getInstance() {
        if (instance == null) {
            instance = new TrackNumberDifference();
        }
        return instance;
    }

	@Override
	public boolean duplicateRuledOut(PactRecord record1, PactRecord record2) {
		int record1DiscTracks = record1.getField(SimilarityMeasure.DISC_TRACKS_FIELD, PactInteger.class).getValue();
		int record2DiscTracks = record2.getField(SimilarityMeasure.DISC_TRACKS_FIELD, PactInteger.class).getValue();
		if(Math.abs(record2DiscTracks-record1DiscTracks)>Math.max(record1DiscTracks, record2DiscTracks)/10.0){
			return true;
		}
		return false;
	}
}
