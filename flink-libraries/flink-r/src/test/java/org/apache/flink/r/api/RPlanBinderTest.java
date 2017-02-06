package org.apache.flink.r.api;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by jens on 21.11.16.
 */
public class RPlanBinderTest extends JavaProgramTestBase {
	@Override
	protected boolean skipCollectionExecution() {
		return true;
	}

	@Override
	public void testJobWithoutObjectReuse() throws Exception {}

	private static String[] whitelist = {"word_count_file.R"};

	private static List<String> findTestFiles() throws Exception {
		List<String> files = new ArrayList<>();
		FileSystem fs = FileSystem.getLocalFileSystem();
		FileStatus[] status = fs.listStatus(
			new Path(fs.getWorkingDirectory().toString()
				+ "/src/test/R/org/apache/flink/R/api"));
		for (FileStatus f : status) {
			String file = f.getPath().toString();
			if (file.endsWith(".R")) {
				files.add(file);
			}
		}
		return files;
	}

	@Override
	protected void testProgram() throws Exception {
		outerloop:
		for (String file : findTestFiles()) {
			for (String incl : whitelist) {
				if (file.endsWith(incl))
					RPlanBinder.main(new String[]{file});
			}
		}
	}
}
