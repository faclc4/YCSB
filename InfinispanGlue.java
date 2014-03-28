import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import org.infinispan.Version;
import org.infinispan.versioning.rmi.RemoteVersionedCache;
import org.infinispan.versioning.rmi.RemoteVersionedCacheImpl;
import org.infinispan.versioning.utils.IncrediblePropertyLoader;
import org.infinispan.versioning.utils.version.VersionScalarGenerator;

/**
 * 
 * @author F��bio Coelho
 */
public class InfinispanGlue extends DB {

	public static final int OK = 0;
	public static final int ERROR = -1;

	private RemoteVersionedCache<String, String> cache;
	VersionScalarGenerator vsg = new VersionScalarGenerator();

	/**
	 * This function is used to intiate the connection to ISNP This function is
	 * executed once per client thread.
	 */
	public void init() throws DBException {
		System.out.println("INITIALIZE THE ISNP CACHE");
		Properties sysProps = System.getProperties();
		IncrediblePropertyLoader.load(sysProps, "rvc-rmi-client.properties");
		String servers = sysProps.getProperty("servers");
		String versioningTechnique = sysProps.getProperty(
				"versioningTechnique", "ATOMICMAP");

		String serviceURL = "//" + server + "/"
				+ RemoteVersionedCacheImpl.SERVICE_NAME + "-"
				+ versioningTechnique;
		System.out.println("Connecting to " + serviceURL + " ...");
		this.cache = (RemoteVersionedCache<String, String>) Naming
				.lookup(serviceURL);

	}

	@Override
	/**
	This function should implement a read
	Table: The cache to be used but it can be disresgarded
	key: the key to be read. Key is selected according to distribution from the dump file.
	version: The version that should be considered when performing a read
	 */
	public int read(String table, String key, Object version) {
		System.out.println("Implement a read function to ISNP");
		Version v = vsg.generateFrom(version);
		try {
			this.cache.get(key, v);
		} catch (RemoteException re) {
			re.printStackTrace();
			return ERROR;
		}
		return OK;
	}

	@Override
	/**
	This function should implement a readRange
	Table: The cache to be use but it can be overrided manually
	key: the key to be read. Key is selected according to distribution from the dump file.
	versionA and versionB: The versions that should be considered when performing a read range
	 */
	public int readRange(String table, String key, Object versionA,
			Object versionB) {
		System.out.println("This is  Readrange");
		System.out.println("key is: " + key);
		System.out.println("versions are:  versionA: " + versionA.toString()
				+ " versionB: " + versionB.toString());
		Version vA = vsg.generateFrom(versionA);
		Version vB = vsg.generateFrom(versionB);
		try{
			cache.get(key, vA, vB);
		} catch (RemoteException re) {
			re.printStackTrace();
			return ERROR;
		}
		return OK;
	}

	@Override
	/**
	This function should a implement a put into ISPN
	Table: can be the cache to use but it can de overrided manually.
	key: the key to be inserted. Key is constructed from the dump file.
	values: a Map of <version,value> to be inserted. version and values are contructed from the dump file.
	 */
	public int update(String table, String key, HashMap<Object, Object> values) {
		System.out.println("This is  update");
		System.out.println("key is: " + key);
		System.out.println("versions are: " + values.toString());
		return OK;
	}

	@Override
	/**
	This function should a implement an insert into ISPN during the LOAD stage of the benchmark.
	Table: can be the cache to use but it can de overrided manually.
	key: the key to be inserted. Key is constructed from the dump file.
	values: a Map of <version,value> to be inserted. version and values are contructed from the dump file.
	 */
	public int insert(String table, String key, HashMap<Object, Object> values) {
		System.out.println("This is a insert");
		System.out.println("key is: " + key);
		System.out.println("versions are: " + values.toString());
		return OK;
	}

	@Override
	/**
	DO NOT CONSIDER THIS FUNCTION- it will never be called.
	 **/
	public int delete(String table, String key) {
		return OK;
	}

}
