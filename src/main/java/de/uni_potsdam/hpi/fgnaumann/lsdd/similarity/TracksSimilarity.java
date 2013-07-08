package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import de.uni_potsdam.hpi.fgnaumann.lsdd.TrackList;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class TracksSimilarity implements PositiveRule {
	private static final int WINDOW_SIZE = 3;

	private static TracksSimilarity instance = null;
	private static AbstractStringMetric dist1 = new Levenshtein();
	private static AbstractStringMetric dist2 = new JaroWinkler();
	private static AbstractStringMetric dist3 = new JaccardSimilarity();

	private TracksSimilarity() {
	}

	public static TracksSimilarity getInstance() {
		if (instance == null) {
			instance = new TracksSimilarity();
		}
		return instance;
	}

	@Override
	public float similarity(PactRecord record1, PactRecord record2) {
		float similarity = 0f;

		TrackList trackList1 = record1.getField(MultiBlocking.TRACKS_FIELD,
				TrackList.class);
		PactRecord[] trackList1Array = new PactRecord[trackList1.size()];
		trackList1.toArray(trackList1Array);

		TrackList trackList2 = record2.getField(MultiBlocking.TRACKS_FIELD,
				TrackList.class);
		PactRecord[] trackList2Array = new PactRecord[trackList2.size()];
		trackList2.toArray(trackList2Array);

		if (trackList1Array.length < trackList2Array.length) {
			PactRecord[] tempSwap = trackList1Array;
			trackList1Array = trackList2Array;
			trackList2Array = tempSwap;
		}

		int i = 0;
		for (; i < trackList1Array.length; i++) {
			String trackNameOuter = trackList1Array[i].getField(
					MultiBlocking.TRACK_TITLE_FIELD, PactString.class)
					.getValue();
			int trackNumberOuter = trackList1Array[i].getField(
					MultiBlocking.TRACK_NUMBER_FIELD, PactInteger.class)
					.getValue();
			float highest = 0f;
			for (int j = 0, k = i - WINDOW_SIZE / 2; j <= WINDOW_SIZE; k++, j++) {
				if (k < 0 || k >= trackList2Array.length)
					continue;
				String trackNameInner = trackList2Array[k].getField(
						MultiBlocking.TRACK_TITLE_FIELD, PactString.class)
						.getValue();
				int trackNumberInner = trackList2Array[k].getField(
						MultiBlocking.TRACK_NUMBER_FIELD, PactInteger.class)
						.getValue();
				
				int trackDifference = (Math.abs(trackNumberOuter - trackNumberInner));
				float differencePunishmentFactor = 0;
				if (trackDifference == 0) {
					differencePunishmentFactor = 1;
				} else if (trackDifference == 1) {
					differencePunishmentFactor = MultiBlocking.SIMILARITY_THRESHOLD + 0.1f;
				} else if (trackDifference == 2) {
					differencePunishmentFactor = MultiBlocking.SIMILARITY_THRESHOLD;
				} else if (trackDifference == 3) {
					differencePunishmentFactor = MultiBlocking.SIMILARITY_THRESHOLD - 0.1f;
				} else {
					differencePunishmentFactor = 0;
				}
				
				float currentSimilarity = ((dist1.getSimilarity(trackNameOuter,
						trackNameInner)
						+ dist2.getSimilarity(trackNameOuter, trackNameInner) + dist3
							.getSimilarity(trackNameOuter, trackNameInner)) / 3)*differencePunishmentFactor;
				//float currentSimilarity = 0.7f;
				if (currentSimilarity > highest)
					highest = currentSimilarity;
			}
			similarity += highest;
		}
		if (i == 0)
			return MultiBlocking.SIMILARITY_THRESHOLD;
		else
			return similarity / i;
	}
	
	@Override
	public int getWeight() {
		return 5;
	}

}
