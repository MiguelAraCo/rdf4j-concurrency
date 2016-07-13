package mx.araco.miguel;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author MiguelAraCo
 */
public class App {
	private static final Logger LOG = LoggerFactory.getLogger( App.class );
	private static final String REPOSITORY_DIRECTORY = "/opt/test/sesame";
	private static Repository repository;
	private static boolean clean = false;
	private static boolean loadDefaultData = false;

	public static void main( String... args ) {
		App test = new App();
		try {
			test.run();
		} catch ( Exception e ) {
			throw new RuntimeException( e );
		}
	}

	private static void initializeRepository() {
		Repository repository = new SailRepository( new NativeStore( new File( REPOSITORY_DIRECTORY ) ) );
		repository.initialize();
		App.repository = repository;
	}

	private static void executeInTransaction( WriteConnectionTemplate action ) {
		String transactionUUID = App.generateUUID();
		try ( RepositoryConnection connection = App.repository.getConnection() ) {
			connection.begin();
			LOG.trace( "Transaction '{}', created.", transactionUUID );
			try {
				action.run( connection );
				LOG.trace( "Action finished, committing transaction '{}'...", transactionUUID );
				connection.commit();
				LOG.trace( "Transaction '{}', was committed.", transactionUUID );
			} catch ( Exception e ) {
				try {
					LOG.warn( "Error while running action. Rolling back transaction '{}'...", transactionUUID );
					connection.rollback();
				} catch ( RepositoryException e2 ) {
					LOG.error( "Transaction '{}', couldn't be rolled back. Exception: {}", transactionUUID, e2 );
				}
			}
		}
	}

	private static <T> T executeInTransaction( ReadConnectionTemplate<T> action ) {
		String transactionUUID = App.generateUUID();
		try ( RepositoryConnection connection = App.repository.getConnection() ) {
			connection.begin();
			LOG.trace( "Transaction '{}', created.", transactionUUID );
			T result = null;
			try {
				result = action.run( connection );
				LOG.trace( "Action finished, committing transaction '{}'...", transactionUUID );
				connection.commit();
				LOG.trace( "Transaction '{}', was committed.", transactionUUID );
			} catch ( Exception e ) {
				try {
					LOG.warn( "Error while running action. Rolling back transaction '{}'...", transactionUUID );
					connection.rollback();
				} catch ( RepositoryException e2 ) {
					LOG.error( "Transaction '{}', couldn't be rolled back. Exception: {}", transactionUUID, e2 );
				}
			}
			return result;
		}
	}

	private static <T> T read( ReadConnectionTemplate<T> action ) throws Exception {
		RepositoryConnection connection = App.repository.getConnection();
		T result = action.run( connection );
		connection.close();
		return result;

	}

	private static long benchmark( Action action ) throws Exception {
		long time = System.nanoTime();
		action.run();
		return System.nanoTime() - time;
	}

	private static <T> BenchmarkTuple<T> benchmark( ActionWithResult<T> action ) throws Exception {
		long time = System.nanoTime();
		T result = action.run();
		time = System.nanoTime() - time;
		return new BenchmarkTuple<>( result, time );
	}

	private static String generateUUID() {
		UUID uuid = UUID.randomUUID();
		return uuid.toString();
	}

	private App() {}

	private void run() throws Exception {
		App.initializeRepository();
		if ( App.clean ) App.executeInTransaction( this::cleanRepository );
		if ( App.loadDefaultData ) App.executeInTransaction( this::loadDefaultData );

		executeConcurrentReadTests( 1 );

		executeConcurrentReadTests( 4 );

		executeConcurrentReadTests( 16 );

		executeConcurrentReadTests( 64 );
	}

	private void executeConcurrentReadTests( int numberOfThreads ) throws Exception {
		LOG.debug( "Executing concurrent read tests with {} threads...", numberOfThreads );

		List<ReadTest> tests = new ArrayList<>();
		ConcurrentLinkedQueue<Long> benchmarks = new ConcurrentLinkedQueue<>();

		for ( int i = 0; i < numberOfThreads; i++ ) {
			tests.add( new ReadTest( benchmarks ) );
		}

		for ( ReadTest test : tests ) {
			test.start();
		}

		for ( ReadTest test : tests ) {
			test.join();
		}

		Statistics statistics = new Statistics( benchmarks );
		LOG.debug( "Concurrent read tests finished, stats: {}", statistics );
	}

	private void loadDefaultData( RepositoryConnection connection ) throws Exception {
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream( "default.ttl" );
		connection.add( inputStream, "https://example.com/", RDFFormat.TURTLE );
	}

	private void cleanRepository( RepositoryConnection connection ) throws Exception {
		connection.remove( (Resource) null, null, null );
	}

	private static class ReadTest extends Thread {
		private final ConcurrentLinkedQueue<Long> benchmarksQueue;

		ReadTest( ConcurrentLinkedQueue<Long> benchmarksQueue ) {
			super();
			this.benchmarksQueue = benchmarksQueue;
		}

		@Override
		public void run() {
			try {
				this.runTest();
			} catch ( Exception e ) {
				throw new RuntimeException( e );
			}
		}

		private void runTest() throws Exception {
			BenchmarkTuple<Integer> benchmark = App.benchmark( () -> App.executeInTransaction( connection -> {
				RepositoryResult<Statement> result = connection.getStatements( null, null, null );
				Integer statements = 0;
				while ( result.hasNext() ) {
					Statement statement = result.next();
					if ( statement != null ) statements++;
				}
				return statements;
			} ) );

			this.benchmarksQueue.add( benchmark.time );
		}
	}

	private static class BenchmarkTuple<T> {
		T result;
		Long time;

		BenchmarkTuple( T result, Long time ) {
			this.result = result;
			this.time = time;
		}
	}

	private static class Statistics<T extends Number> {
		private Double maximum;
		private Double minimum;
		private double mean;
		private double variance;
		private double standardDeviation;

		public Statistics( Collection<Number> data ) {
			int size = data.size();

			this.mean = 0.0;
			for ( Number number : data ) {
				double doubleValue = number.doubleValue();
				if ( this.maximum == null || this.maximum < doubleValue ) this.maximum = doubleValue;
				if ( this.minimum == null || this.minimum > doubleValue ) this.minimum = doubleValue;
				this.mean += doubleValue;
			}
			this.mean = this.mean / size;

			this.variance = 0.0;
			for ( Number number : data ) {
				double doubleValue = number.doubleValue();
				this.variance += ( this.mean - doubleValue ) * ( this.mean - doubleValue );
			}
			this.variance = this.variance / size;

			this.standardDeviation = Math.sqrt( this.variance );
		}

		public Double getMaximum() {return maximum;}

		public Double getMinimum() {return minimum;}

		public double getMean() {return mean;}

		public double getVariance() {return variance;}

		public double getStandardDeviation() {return standardDeviation;}

		@Override
		public String toString() {
			return ""
				+ "\n\tMax:\t\t" + ( new DecimalFormat( "#,###,###,###,###,###,###,###" ) ).format( maximum )
				+ "\n\tMin:\t\t" + ( new DecimalFormat( "#,###,###,###,###,###,###,###" ) ).format( minimum )
				+ "\n\tMean:\t\t" + ( new DecimalFormat( "#,###,###,###,###,###,###,###" ) ).format( mean )
				+ "\n\tStdDev:\t\t" + ( new DecimalFormat( "#,###,###,###,###,###,###,###" ) ).format( standardDeviation )
				;
		}
	}

	@FunctionalInterface
	private interface WriteConnectionTemplate {
		void run( RepositoryConnection connection ) throws Exception;
	}

	@FunctionalInterface
	private interface ReadConnectionTemplate<T> {
		T run( RepositoryConnection connection ) throws Exception;
	}

	@FunctionalInterface
	private interface Action {
		void run() throws Exception;
	}

	@FunctionalInterface
	private interface ActionWithResult<T> {
		T run() throws Exception;
	}
}
