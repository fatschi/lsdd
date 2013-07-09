package de.uni_potsdam.hpi.fgnaumann.lsdd.similarity;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaccardSimilarity;
import de.uni_potsdam.hpi.fgnaumann.lsdd.MultiBlocking;
import de.uni_potsdam.hpi.fgnaumann.lsdd.TrackList;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class TracksSimilarity implements PositiveRule {
	private static final int WINDOW_SIZE = 3;

	private static TracksSimilarity instance = null;
	private static AbstractStringMetric distance = new JaccardSimilarity();

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
		if (trackList1.size() == 0)
			return 0f;
		PactRecord[] trackList1Array = new PactRecord[trackList1.size()];
		trackList1.toArray(trackList1Array);

		TrackList trackList2 = record2.getField(MultiBlocking.TRACKS_FIELD,
				TrackList.class);
		if (trackList2.size() == 0)
			return 0f;
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

				int trackDifference = Math.abs(trackNumberOuter
						- trackNumberInner);
				final int maxDifference = Math.min(trackNumberOuter,
						trackNumberInner);
				float differencePunishmentFactor = 0;
				if (trackDifference <= maxDifference) {
					differencePunishmentFactor = 1 - trackDifference
							/ maxDifference;
				}

				float currentSimilarity = ((distance.getSimilarity(trackNameOuter,
						trackNameInner)) / 1) * differencePunishmentFactor;
				// float currentSimilarity = 0.7f;
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
