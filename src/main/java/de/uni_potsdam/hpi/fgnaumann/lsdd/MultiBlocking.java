/**
 * 
 */

package de.uni_potsdam.hpi.fgnaumann.lsdd;

import java.util.Collection;
import java.util.HashSet;

import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.BalancedBlockFilterStep;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.CoGroupCDsWithTracks;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.CountClosureSizeStep;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.CountOutputStep;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.CountStep;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.FirstBlockingStep;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.MatchStep;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.SortedNeighbourhood;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.TransitiveClosureStep;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.UnbalancedBlockFilterStep;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.UnionStep;
import de.uni_potsdam.hpi.fgnaumann.lsdd.stubs.ValidatorStep;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;

/**
 * A Stratosphere-Pact-Implementation of "A fast approach for parallel
 * deduplication on multicore processors" - Guilherme Dal Bianco et al. with
 * variations in big block handling.
 * 
 * @author fabian.tschirschnitz@student.hpi.uni-potsdam.de
 * 
 */
public class MultiBlocking implements PlanAssembler, PlanAssemblerDescription {
	// record field indizes
	public static final int DISC_ID_FIELD = 0;
	public static final int DISC_FREEDB_ID_FIELD = 1;
	public static final int ARTIST_NAME_FIELD = 2;
	public static final int DISC_TITLE_FIELD = 3;
	public static final int GENRE_TITLE_FIELD = 4;
	public static final int DISC_RELEASED_FIELD = 5;
	public static final int DISC_TRACKS_FIELD = 6;
	public static final int DISC_SECONDS_FIELD = 7;
	public static final int DISC_LANGUAGE_FIELD = 8;
	public static final int TRACKS_FIELD = 9;
	public static final int BLOCKING_KEY_FIELD = 10;
	public static final int BLOCKING_ID_FIELD = 11;
	public static final int BLOCKING_KEY_EXTENDED_FIELD = 12;
	public static final int COUNT_FIELD = 13;
	public static final int TRACK_NUMBER_FIELD = 1;
	public static final int TRACK_TITLE_FIELD = 2;
	public static final int DUPLICATE_ID_1_FIELD = 0;
	public static final int DUPLICATE_ID_2_FIELD = 1;
	public static final int DUPLICATE_REDUCE_FIELD = 2;

	// stats
	private static int MAX_BLOCK_SIZE = 16352;
	// parameters
	public static int MAX_WINDOW_FOR_LARGE_BLOCKS = 5;
	public static int MAX_WINDOW_SIZE = 25;
	public static float SIMILARITY_THRESHOLD = 0.8f;
	public static boolean takeTracksIntoAccount = true;
	public static boolean buildTransitveClosure = false;
	public static boolean outputBlockSizes = false;
	public static boolean outputClosureSizes = false;

	public static int MAXIMUM_COMPARISON = MAX_WINDOW_FOR_LARGE_BLOCKS
			* MAX_BLOCK_SIZE;
	public static int THRESHOLD = (int) Math.round(Math
			.sqrt(MAXIMUM_COMPARISON));

	@Override
	public Plan getPlan(final String... args) {
		Collection<GenericDataSink> sinks = new HashSet<GenericDataSink>();

		final int noSubtasks;
		final String inputFileDiscs;
		final String inputFileTracks;
		final String output;
		final String inputFileGold;
		if (args.length >= 4) {
			noSubtasks = Integer.parseInt(args[0]);
			inputFileDiscs = args[1];
			inputFileTracks = args[2];
			inputFileGold = args[3];
			output = args[4];
		} else {
			throw new IllegalArgumentException(
					"The number of given parameters is wrong!");
		}

		// create DataSourceContract for discs input
		// disc_id;freedbdiscid;"artist_name";"disc_title";"genre_title";"disc_released";disc_tracks;disc_seconds;"disc_language"
		// 7;727827;"Tenacious D";"Tenacious D";"Humour";"2001";19;2843;"eng"
		FileDataSource discs = new FileDataSource(DiscsInputFormat.class,
				inputFileDiscs, "discs");
		DiscsInputFormat.configureRecordFormat(discs).recordDelimiter('\n')
				.fieldDelimiter(';')
				.field(DecimalTextIntParser.class, DISC_ID_FIELD) // disc_id
				.field(DecimalTextIntParser.class, DISC_FREEDB_ID_FIELD) // freedbdiscid
				.field(VarLengthStringParser.class, ARTIST_NAME_FIELD) // "artist_name"
				.field(VarLengthStringParser.class, DISC_TITLE_FIELD) // "disc_title"
				.field(VarLengthStringParser.class, GENRE_TITLE_FIELD) // "genre_title"
				.field(VarLengthStringParser.class, DISC_RELEASED_FIELD) // "disc_released"
				.field(DecimalTextIntParser.class, DISC_TRACKS_FIELD) // disc_tracks
				.field(DecimalTextIntParser.class, DISC_SECONDS_FIELD) // disc_seconds
				.field(VarLengthStringParser.class, DISC_LANGUAGE_FIELD); // "disc_language"

		FileDataSource tracks = null;
		if (takeTracksIntoAccount) {
			// create DataSourceContract for tracks input
			// disc_id;track_number;"track_title";"artist_name";track_seconds
			// 2;1;"Intro+Chor Der Kriminalbeamten";"Kottans Kapelle";115
			tracks = new FileDataSource(RecordInputFormat.class,
					inputFileTracks, "tracks");
			RecordInputFormat.configureRecordFormat(tracks)
					.recordDelimiter('\n').fieldDelimiter(';')
					.field(DecimalTextIntParser.class, DISC_ID_FIELD) // disc_id
					.field(DecimalTextIntParser.class, 1) // track_number
					.field(VarLengthStringParser.class, TRACK_TITLE_FIELD) // "track_title"
					.field(VarLengthStringParser.class, 3) // "artist_name"
					.field(DecimalTextIntParser.class, 4); // track_seconds
		}

		// create DataSourceContract for gold standard input
		// disc_id1;disc_id2
		// 13;3163
		FileDataSource gold = new FileDataSource(RecordInputFormat.class,
				inputFileGold, "silver standard");
		RecordInputFormat.configureRecordFormat(gold).recordDelimiter('\n')
				.fieldDelimiter(';')
				.field(DecimalTextIntParser.class, DUPLICATE_ID_1_FIELD) // disc_id1
				.field(DecimalTextIntParser.class, DUPLICATE_ID_2_FIELD); // disc_id1

		// contracts
		MapContract firstBlockingStep;

		if (takeTracksIntoAccount) {
			CoGroupContract coGrouper = CoGroupContract
					.builder(CoGroupCDsWithTracks.class, PactInteger.class, 0,
							0).name("join cds with tracks").build();

			coGrouper.setFirstInput(discs);
			coGrouper.setSecondInput(tracks);

			firstBlockingStep = MapContract.builder(FirstBlockingStep.class)
					.input(coGrouper)
					.name("first blocking step without track list").build();
		} else {
			firstBlockingStep = MapContract.builder(FirstBlockingStep.class)
					.input(discs)
					.name("first blocking step without track list").build();
		}

		ReduceContract countStep = new ReduceContract.Builder(CountStep.class,
				PactString.class, BLOCKING_KEY_FIELD)
				.keyField(PactString.class, BLOCKING_ID_FIELD)
				.input(firstBlockingStep).name("count records step").build();

		MapContract unbalancedBlockFilter = MapContract
				.builder(UnbalancedBlockFilterStep.class).input(countStep)
				.name("filter unbalanced blocks step").build();

		MapContract balancedBlockFilter = MapContract
				.builder(BalancedBlockFilterStep.class).input(countStep)
				.name("filter balanced blocks step").build();

		ReduceContract matchStepReducerBalanced = new ReduceContract.Builder(
				MatchStep.class, PactString.class, BLOCKING_KEY_FIELD)
				.keyField(PactString.class, BLOCKING_ID_FIELD)
				.input(unbalancedBlockFilter).name("match step balanced")
				.build();

		ReduceContract unionStep1 = new ReduceContract.Builder(UnionStep.class,
				PactInteger.class, DUPLICATE_ID_1_FIELD)
				.keyField(PactInteger.class, DUPLICATE_ID_2_FIELD)
				.input(matchStepReducerBalanced).name("union step 1").build();

		ReduceContract sortedNeighbourhoodStep = new ReduceContract.Builder(
				SortedNeighbourhood.class, PactString.class, BLOCKING_KEY_FIELD)
				.keyField(PactString.class, BLOCKING_ID_FIELD)
				.secondaryOrder(
						new Ordering(BLOCKING_KEY_EXTENDED_FIELD,
								PactString.class, Order.ASCENDING))
				.input(balancedBlockFilter).name("second blocking step")
				.build();

		ReduceContract unionStep2 = new ReduceContract.Builder(UnionStep.class,
				PactInteger.class, DUPLICATE_ID_1_FIELD)
				.keyField(PactInteger.class, DUPLICATE_ID_2_FIELD)
				.input(sortedNeighbourhoodStep).name("union step 2").build();
		unionStep2.addInput(unionStep1);
		unionStep2.setDegreeOfParallelism(1);

		MatchContract validatorStep;
		FileDataSink outResult;

		if (buildTransitveClosure) {
			ReduceContract transitiveClosureStep = new ReduceContract.Builder(
					TransitiveClosureStep.class, PactInteger.class,
					DUPLICATE_REDUCE_FIELD).input(unionStep2)
					.name("transitive closure").build();
			unionStep2.setDegreeOfParallelism(1);

			validatorStep = MatchContract
					.builder(ValidatorStep.class, PactInteger.class,
							DUPLICATE_ID_1_FIELD, DUPLICATE_ID_1_FIELD)
					.input1(transitiveClosureStep)
					.input2(gold)
					.name("validator step")
					.keyField(PactInteger.class, DUPLICATE_ID_2_FIELD,
							DUPLICATE_ID_2_FIELD).build();

			// file output result
			outResult = new FileDataSink(RecordOutputFormat.class, output
					+ "/result.csv", transitiveClosureStep, "Output Result");
			RecordOutputFormat.configureRecordFormat(outResult)
					.recordDelimiter('\n').fieldDelimiter(' ').lenient(true)
					.field(PactInteger.class, DUPLICATE_ID_1_FIELD)
					.field(PactInteger.class, DUPLICATE_ID_2_FIELD)
					.field(PactInteger.class, DUPLICATE_REDUCE_FIELD);
			outResult.setDegreeOfParallelism(1);

			if (outputClosureSizes) {
				// statistics contracts
				ReduceContract countClosureSizeStep = new ReduceContract.Builder(
						CountClosureSizeStep.class, PactInteger.class,
						DUPLICATE_REDUCE_FIELD).input(transitiveClosureStep)
						.name("count closure size step").build();

				FileDataSink countClosureSizeOutput = new FileDataSink(
						RecordOutputFormat.class, output + "/closure_size.csv",
						countClosureSizeStep, "output block sizes");

				RecordOutputFormat
						.configureRecordFormat(countClosureSizeOutput)
						.recordDelimiter('\n').fieldDelimiter(';')
						.lenient(true).field(PactInteger.class, 0)
						.field(PactInteger.class, 1);
				countClosureSizeOutput.setDegreeOfParallelism(1);

				sinks.add(countClosureSizeOutput);
			}
		} else {
			validatorStep = MatchContract
					.builder(ValidatorStep.class, PactInteger.class,
							DUPLICATE_ID_1_FIELD, DUPLICATE_ID_1_FIELD)
					.input1(unionStep2)
					.input2(gold)
					.name("validator step")
					.keyField(PactInteger.class, DUPLICATE_ID_2_FIELD,
							DUPLICATE_ID_2_FIELD).build();

			outResult = new FileDataSink(RecordOutputFormat.class, output
					+ "/result.csv", unionStep2, "Output Result");
			RecordOutputFormat.configureRecordFormat(outResult)
					.recordDelimiter('\n').fieldDelimiter(' ').lenient(true)
					.field(PactInteger.class, DUPLICATE_ID_1_FIELD)
					.field(PactInteger.class, DUPLICATE_ID_2_FIELD)
					.field(PactInteger.class, DUPLICATE_REDUCE_FIELD);
			outResult.setDegreeOfParallelism(1);
		}

		if (outputBlockSizes) {
			// debugging contracts
			ReduceContract countOutputStep = new ReduceContract.Builder(
					CountOutputStep.class, PactInteger.class, COUNT_FIELD)
					.keyField(PactString.class, BLOCKING_ID_FIELD)
					.keyField(PactString.class, BLOCKING_KEY_FIELD)
					.input(countStep).name("count output step").build();

			FileDataSink countOutput = new FileDataSink(
					RecordOutputFormat.class, output + "/block_size.csv",
					countOutputStep, "output block sizes");

			// debug outputs
			// count output steps
			RecordOutputFormat.configureRecordFormat(countOutput)
					.recordDelimiter('\n').fieldDelimiter(';').lenient(true)
					.field(PactInteger.class, 0).field(PactString.class, 1)
					.field(PactString.class, 2);
			countOutput.setDegreeOfParallelism(1);

			sinks.add(countOutput);
		}

		// file output tp
		FileDataSink outTruePositives = new FileDataSink(
				RecordOutputFormat.class, output + "/tp.csv", validatorStep,
				"output true positives");
		RecordOutputFormat.configureRecordFormat(outTruePositives)
				.recordDelimiter('\n').fieldDelimiter(' ').lenient(true)
				.field(PactInteger.class, DUPLICATE_ID_1_FIELD)
				.field(PactInteger.class, DUPLICATE_ID_2_FIELD)
				.field(PactInteger.class, DUPLICATE_REDUCE_FIELD);
		outTruePositives.setDegreeOfParallelism(1);

		// assemble the PACT plan
		sinks.add(outResult);
		sinks.add(outTruePositives);
		// sinks.add(blocksOutput);
		Plan plan = new Plan(sinks, "multiblocking");
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
}
