
public class Driver {
	public static void main(String[] args) throws Exception {
		
		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Normalize normalize = new Normalize();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();
		RemoveRatedMovies removeRatedMovies = new RemoveRatedMovies();
		Top10MoviesRecom top10MoviesRecom = new Top10MoviesRecom();

		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String normalizeDir = args[3];
		String multiplicationDir = args[4];
		String sumDir = args[5];
		String removeRatedMoviesDir = args[6];
		String top10MoviesRecomDir = args[7];
		String[] path1 = {rawInput, userMovieListOutputDir};
		String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {coOccurrenceMatrixDir, normalizeDir};
		String[] path4 = {normalizeDir, rawInput, multiplicationDir};
		String[] path5 = {multiplicationDir, sumDir};
		String[] path6 = {rawInput, sumDir, removeRatedMoviesDir};
		String[] path7 = {removeRatedMoviesDir, top10MoviesRecomDir};
		
		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		normalize.main(path3);
		multiplication.main(path4);
		sum.main(path5);
		removeRatedMovies.main(path6);
		top10MoviesRecom.main(path7);
	}

}
