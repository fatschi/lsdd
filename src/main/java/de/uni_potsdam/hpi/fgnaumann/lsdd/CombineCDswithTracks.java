/**
 * 
 */

package de.uni_potsdam.hpi.fgnaumann.lsdd;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;
import eu.stratosphere.sopremo.pact.JsonOutputFormat;

/**
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class CombineCDswithTracks implements PlanAssembler,
		PlanAssemblerDescription {
	public static final int THRESHOLD = 250;

	@Override
	public Plan getPlan(final String... args) {
		// parse program parameters
		/*
		 * 4 file:///home/fabian/lsdd/data/mini.csv
		 * file:///home/fabian/lsdd/data/freedb_tracks.csv
		 * file:///home/fabian/lsdd/out
		 */
		final int noSubtasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String inputFileDiscs = (args.length > 1 ? args[1] : "");
		final String inputFileTracks = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");

		// create DataSourceContract for discs input
		// disc_id;freedbdiscid;"artist_name";"disc_title";"genre_title";"disc_released";disc_tracks;disc_seconds;"disc_language"
		// 7;727827;"Tenacious D";"Tenacious D";"Humour";"2001";19;2843;"eng"
		FileDataSource discs = new FileDataSource(RecordInputFormat.class,
				inputFileDiscs, "Discs");
		RecordInputFormat.configureRecordFormat(discs).recordDelimiter('\n')
				.fieldDelimiter(';').field(DecimalTextIntParser.class, 0) // disc_id
				.field(DecimalTextIntParser.class, 1) // freedbdiscid
				.field(VarLengthStringParser.class, 2) // "artist_name"
				.field(VarLengthStringParser.class, 3) // "disc_title"
				.field(VarLengthStringParser.class, 4) // "genre_title"
				.field(VarLengthStringParser.class, 5) // "disc_released"
				.field(DecimalTextIntParser.class, 6) // disc_tracks
				.field(DecimalTextIntParser.class, 7) // disc_seconds
				.field(VarLengthStringParser.class, 8); // "disc_language"

		// create DataSourceContract for tracks input
		// disc_id;track_number;"track_title";"artist_name";track_seconds
		// 2;1;"Intro+Chor Der Kriminalbeamten";"Kottans Kapelle";115
		FileDataSource tracks = new FileDataSource(RecordInputFormat.class,
				inputFileTracks, "Tracks");
		RecordInputFormat.configureRecordFormat(tracks).recordDelimiter('\n')
				.fieldDelimiter(';').field(DecimalTextIntParser.class, 0) // disc_id
				.field(DecimalTextIntParser.class, 1) // freedbdiscid
				.field(VarLengthStringParser.class, 2) // "track_title"
				.field(VarLengthStringParser.class, 3) // "artist_name"
				.field(DecimalTextIntParser.class, 4); // track_seconds

		//
		CoGroupContract coGrouper = CoGroupContract
				.builder(CoGroupCDsWithTracks.class, PactInteger.class, 0, 0)
				.name("Group CDs with Tracks").build();

		coGrouper.setFirstInput(discs);
		coGrouper.setSecondInput(tracks);

		FileDataSink outCDsWithTracks = new FileDataSink(
				RecordOutputFormat.class, output, coGrouper,
				"Group CDs with Tracks");

		JsonOutputFormat.configureFileFormat(outCDsWithTracks);
				/*.field(PactInteger.class, 0) // disc_id
				.field(PactInteger.class, 1) // freedbdiscid
				.field(PactString.class, 2) // "artist_name"
				.field(PactString.class, 3) // "disc_title"
				.field(PactString.class, 4) // "genre_title"
				.field(PactString.class, 5) // "disc_released"
				.field(PactInteger.class, 6) // disc_tracks
				.field(PactInteger.class, 7) // disc_seconds
				.field(PactString.class, 8)// disc_seconds
				.field(TrackList.class, 9); // tracks*/

		// assemble the PACT plan
		Plan plan = new Plan(outCDsWithTracks, "Group CDs with Tracks");
		plan.setDefaultParallelism(noSubtasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks], [discs], [tracks], [output]";
	}

	public static class CoGroupCDsWithTracks extends CoGroupStub {

		@Override
		public void coGroup(Iterator<PactRecord> inputRecords,
				Iterator<PactRecord> concatRecords, Collector<PactRecord> out) {
			while (inputRecords.hasNext()) {
				PactRecord outputRecord = inputRecords.next().createCopy();
				TrackList trackList = new TrackList();
				while (concatRecords.hasNext()) {
					trackList.add(concatRecords.next().createCopy());
				}
				outputRecord.addField(trackList);
				out.collect(outputRecord);
			}

		}
	}
}
