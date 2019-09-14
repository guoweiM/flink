/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn;

import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for verifying file staging during submission to YARN works.
 */
public class YarnFileStageTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder CLASS_TEMP_DIR = new TemporaryFolder();

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;

	private static Path hdfsRootPath;

	private org.apache.hadoop.conf.Configuration hadoopConfig;

	// ------------------------------------------------------------------------
	//  Test setup and shutdown
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void createHDFS() throws Exception {
		Assume.assumeTrue(!OperatingSystem.isWindows());

		final File tempDir = CLASS_TEMP_DIR.newFolder();

		org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();
		hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());

		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
		hdfsCluster = builder.build();
		hdfsRootPath = new Path(hdfsCluster.getURI());
	}

	@AfterClass
	public static void destroyHDFS() {
		if (hdfsCluster != null) {
			hdfsCluster.shutdown();
		}
		hdfsCluster = null;
		hdfsRootPath = null;
	}

	@Before
	public void initConfig() {
		hadoopConfig = new org.apache.hadoop.conf.Configuration();
		hadoopConfig.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, hdfsRootPath.toString());
	}

	/**
	 * Verifies that nested directories are properly copied with a <tt>hdfs://</tt> file
	 * system (from a <tt>file:///absolute/path</tt> source path).
	 */
	@Test
	public void testCopyFromLocalRecursiveWithScheme() throws Exception {
		final FileSystem targetFileSystem = hdfsRootPath.getFileSystem(hadoopConfig);
		final Path targetDir = targetFileSystem.getWorkingDirectory();

		testCopyFromLocalRecursive(targetFileSystem, targetDir, tempFolder,  false, true);
	}

	/**
	 * Verifies that nested directories are properly copied with a <tt>hdfs://</tt> file
	 * system (from a <tt>/absolute/path</tt> source path).
	 */
	@Test
	public void testCopyFromLocalRecursiveWithoutScheme() throws Exception {
		final FileSystem targetFileSystem = hdfsRootPath.getFileSystem(hadoopConfig);
		final Path targetDir = targetFileSystem.getWorkingDirectory();

		testCopyFromLocalRecursive(targetFileSystem, targetDir, tempFolder, false, false);
	}

	/**
	 * Verifies that nested directories are properly copied to the
	 * {@value org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever#DEFAULT_JOB_DIR} directory
	 * with a <tt>hdfs://</tt> file system (from a <tt>/absolute/path</tt> source path).
	 */
	@Test
	public void testShipFilesDeployToJobDir() throws Exception {
		final FileSystem targetFileSystem = hdfsRootPath.getFileSystem(hadoopConfig);
		final Path targetDir = targetFileSystem.getWorkingDirectory();
		testCopyFromLocalRecursive(targetFileSystem, targetDir, tempFolder, true, false);
	}

	/**
	 * Verifies that nested directories are properly copied with the given filesystem and paths.
	 *
	 * @param targetFileSystem
	 * 		file system of the target path
	 * @param targetDir
	 * 		target path (URI like <tt>hdfs://...</tt>)
	 * @param tempFolder
	 * 		JUnit temporary folder rule to create the source directory with
	 * @param addSchemeToLocalPath
	 * 		whether add the <tt>file://</tt> scheme to the local path to copy from
	 * @param deployToJobDir
	 * 		whether the shipFiles will deploy to the directory
	 * 	  	{@value org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever#DEFAULT_JOB_DIR}
	 */
	static void testCopyFromLocalRecursive(
			FileSystem targetFileSystem,
			Path targetDir,
			TemporaryFolder tempFolder,
			boolean deployToJobDir,
			boolean addSchemeToLocalPath) throws Exception {

		// directory must not yet exist
		assertFalse(targetFileSystem.exists(targetDir));

		final Path singleFilePath; // the file is not in the srcDir directory
		final String singleFileName = "single.jar";
		final String singleFileContent = "single file content";
		final String singleFileAbsolutePath = tempFolder.getRoot().getAbsolutePath() + "/" + singleFileName;

		final File srcDir = tempFolder.newFolder();
		final Path srcPath;
		if (addSchemeToLocalPath) {
			srcPath = new Path("file://" + srcDir.getAbsolutePath());
			singleFilePath = new Path("file://" + singleFileAbsolutePath);

		} else {
			srcPath = new Path(srcDir.getAbsolutePath());
			singleFilePath = new Path(singleFileAbsolutePath);
		}

		HashMap<String /* (relative) path */, /* contents */ String> srcFiles = new HashMap<>(4);

		// create and fill source files
		srcFiles.put(srcDir.getName() + "/1", "Hello 1");
		srcFiles.put(srcDir.getName() + "/2", "Hello 2");
		srcFiles.put(srcDir.getName() + "/nested/3", "Hello nested/3");
		srcFiles.put(srcDir.getName() + "/nested/4/5", "Hello nested/4/5");
		srcFiles.put(srcDir.getName() + "/test.jar", "JAR Content");

		for (Map.Entry<String, String> src : srcFiles.entrySet()) {
			File file = new File(tempFolder.getRoot().toString(), src.getKey());
			//noinspection ResultOfMethodCallIgnored
			file.getParentFile().mkdirs();
			try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
				out.writeUTF(src.getValue());
			}
		}

		//create and fill the single file which is not in the srcDir
		try (DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(singleFileAbsolutePath)))) {
			out.writeUTF(singleFileContent);
		}
		// copy the created directory recursively:
		try {
			List<Path> remotePaths = new ArrayList<>();
			HashMap<String, LocalResource> localResources = new HashMap<>();
			final List<String> classpath = AbstractYarnClusterDescriptor.uploadAndRegisterFiles(
				Arrays.asList(new File(srcPath.toUri().getPath()), new File(singleFilePath.toUri().getPath())),
				targetFileSystem,
				targetDir,
				ApplicationId.newInstance(0, 0),
				remotePaths,
				localResources,
				deployToJobDir,
				new StringBuilder());

			if (deployToJobDir) {
				//the classpath is empty if deployToJobDir is true.
				assertEquals(0, classpath.size());
			} else {
				assertThat(
					classpath,
					Matchers.containsInAnyOrder(
						srcDir.getName(),
						srcDir.getName() + "/nested",
						srcDir.getName() + "/nested/4",
						srcDir.getName() + "/test.jar",
						singleFilePath.getName()));
			}

			assertEquals(srcFiles.size() + 1 /* include the singleFile */, localResources.size());

			Path workDir;
			if (deployToJobDir) {
				workDir = ConverterUtils
					.getPathFromYarnURL(
						localResources.get(
							FileJobGraphRetriever.DEFAULT_JOB_DIR + "/" + singleFilePath.getName()).getResource())
					.getParent();
			} else {
				workDir = ConverterUtils
					.getPathFromYarnURL(
						localResources.get(singleFilePath.getName()).getResource())
					.getParent();
			}

			HashMap<String /* (relative) path */, /* contents */ String> allExpectedFileContents =
				new HashMap<>(5);
			// merge the contents of the single file and all files in the srcPath directory
			allExpectedFileContents.putAll(srcFiles);
			allExpectedFileContents.put(singleFilePath.getName(), singleFileContent);

			RemoteIterator<LocatedFileStatus> targetFilesIterator =
				targetFileSystem.listFiles(workDir, true);
			HashMap<String /* (relative) path */, /* contents */ String> targetFiles =
				new HashMap<>(5);

			final int workDirPrefixLength =
				workDir.toString().length() + 1; // one more for the concluding "/"
			while (targetFilesIterator.hasNext()) {
				LocatedFileStatus targetFile = targetFilesIterator.next();
				int retries = 5;
				do {
					try (FSDataInputStream in = targetFileSystem.open(targetFile.getPath())) {
						String absolutePathString = targetFile.getPath().toString();
						String relativePath = absolutePathString.substring(workDirPrefixLength);
						targetFiles.put(relativePath, in.readUTF());
						assertEquals("extraneous data in file " + relativePath, -1, in.read());
						break;
					} catch (FileNotFoundException e) {
						// For S3, read-after-write may be eventually consistent, i.e. when trying
						// to access the object before writing it; see
						// https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel
						// -> try again a bit later
						Thread.sleep(50);
					}
				} while ((retries--) > 0);
			}

			assertThat(targetFiles, equalTo(allExpectedFileContents));
		} finally {
			// clean up
			targetFileSystem.delete(targetDir, true);
		}
	}
}
