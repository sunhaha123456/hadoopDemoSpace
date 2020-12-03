package hadoop.demo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * 功能：使用API 进行HDFS 基本操作
 */
public class HDFS_01 {

	// 备注："hdfs://aly:9000" 和 "hdfs://xxx.xxx.xxx.xxx:9000" 效果完全一致
	public static final String HDFS_PATH = "hdfs://aly:9000";

	Configuration configuration = null;
	FileSystem fileSystem = null;

	@Before
	public void setUp() throws Exception {
		configuration = new Configuration();

		// 此配置，会在访问 dataNode时，使 nameNode 返回 dataNode 的域名，需要在本地hosts文件中，添加 dataNode IP 映射，把 nameNode IP 映射一并添加
		// 否则，会返回dataNode 内网IP，导致本地链接 dataNode 失败
		configuration.set("dfs.client.use.datanode.hostname", "true");
		// 设置副本系数为 1，默认下是 3，也就是创建文件时，会指定为创建 3 份
		configuration.set("dfs.replication", "1");

		// 注意指定用户，需要查看hdfs中文件的用户权限
		fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
	}

	// 创建HDFS文件夹
	@Test
	public void mkdirTest() throws Exception {
		System.out.println(fileSystem.mkdirs(new Path("/hello")));
	}

	// 查看HDFS文件内容
	@Test
	public void text_01() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/alyTest.txt"));
		IOUtils.copyBytes(in, System.out, 1024);
	}

	// 查看HDFS文件内容
	@Test
	public void text_02() throws Exception {
		FSDataInputStream in = fileSystem.open(new Path("/alyTest.txt"));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		String line = null;
		while ((line = reader.readLine()) != null) {
			System.out.println(line);
		}
	}

	// 创建HDFS文件
	@Test
	public void create() throws Exception {
		boolean flag = fileSystem.exists(new Path("/aaa.txt"));
		System.out.println(flag ? "exist" : "not exist");

		// 如果hadoop 中文件已存在，每次执行，都会覆盖掉原先的内容
		FSDataOutputStream out = fileSystem.create(new Path("/aaa.txt"));
		out.writeUTF("hello hahaha");
		out.flush();
		out.close();
	}

	// HDFS文件更命名
	@Test
	public void rename() throws Exception {
		Path oldPath = new Path("/aaa.txt");
		Path newPath = new Path("/aaa-new.txt");
		fileSystem.rename(oldPath, newPath);
	}

	// 上传本地文件到HDFS
	@Test
	public void copyFromLocalFile() throws Exception {
		// 将本地文件内容 覆盖掉 目标文件内容
		Path src = new Path("F:\\testSpace\\abc.txt");
		//Path desc = new Path("/");
		Path desc = new Path("/aaa-new.txt");
		fileSystem.copyFromLocalFile(src, desc);
	}

	// 上传本地大文件到HDFS，带进度条
	@Test
	public void copyFromLocalBigFile() throws Exception {
		InputStream in = new BufferedInputStream(new FileInputStream(new File("D:\\JDK1.8\\src.zip")));

		FSDataOutputStream out = fileSystem.create(new Path("/src.zip"),
				new Progressable() {
					@Override
					public void progress() {
						System.out.print("=");
					}
				}
		);

		IOUtils.copyBytes(in, out, 1024);
	}

	// 下载HDFS文件到本地
	@Test
	public void copyToLocal() throws Exception {
		// 将本地文件内容 覆盖掉 目标文件内容
		Path src = new Path("/aaa-new.txt" );
		Path desc = new Path("F:\\testSpace\\aaa-new-local.txt");
		fileSystem.copyToLocalFile(src, desc);
	}


	// 获取HDFS指定目录下的所有文件列表
	@Test
	public void listFiles() throws Exception {
		// 只列出当前的目录下的
		FileStatus[] statuses = fileSystem.listStatus(new Path("/hello"));
		for (FileStatus s : statuses) {
			System.out.println(s.getPath());
			System.out.println(s.isDirectory());
			System.out.println(s.getPermission().toString());
			System.out.println(s.getReplication());
			System.out.println(s.getLen());
		}

		System.out.println("=========================");

		// 递归列出当前的目录下的所有的文件（只会列出所有文件，不包含目录）
		RemoteIterator<LocatedFileStatus> fileList = fileSystem.listFiles(new Path("/"), true);
		LocatedFileStatus f = null;
		while (fileList.hasNext()) {
			f = fileList.next();
			System.out.println(f.getPath());
		}
	}

	// 查看HDFS文件block块信息
    @Test
    public void getFileBlockLocation() throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/aaa-new.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for (BlockLocation block: blocks) {
            for (String name: block.getNames()) {
                System.out.println(name + "     " + block.getOffset() + " : " + block.getLength());
            }
        }
	}

	// 删除文件
	@Test
	public void delete() throws Exception {
		// 当删除的是非空目录时，需要开启递归删除，其他情况下，可以不开启
		boolean result = fileSystem.delete(new Path("/hello"), true);
		System.out.println(result);
	}
}