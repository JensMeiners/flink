package org.apache.flink.r.api;

/**
 * Created by jens on 22.03.17.
 */
public class RBenchmarks {
	public static void main(String[] args) {
		String[] scripts = args[1].split(",");
		int count = new Integer(args[2]);
		for (String script : scripts) {
			System.out.println("> script: "+script);
			for (int i=0; i < count; i++) {
				System.out.println(">> run: "+i);
				try {
					run(script);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void run(String script) throws Exception {
		long start = System.currentTimeMillis();
		RPlanBinder.main(new String[]{script});
		long end = System.currentTimeMillis();
		System.out.println(">>> "+(end-start));
	}
}
