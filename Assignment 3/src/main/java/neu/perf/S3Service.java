package neu.perf;
import java.io.File;
import java.util.List;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Utility functions for working with S3
 * 
 * @author Joe Sackett @maintainer Jan Vitek
 */
class S3Service {

	private AmazonS3Client client;

	S3Service(String regionName) {
		client = new AmazonS3Client(new ProfileCredentialsProvider().getCredentials());
		client.setRegion(Region.getRegion(Regions.fromName(regionName)));
	}

	boolean bucketExists(String bucketName) {
		return client.doesBucketExist(removeS3(bucketName));
	}

	boolean folderExists(String fullPath) {
		String path = removeS3(fullPath);
		String bucket = path.substring(0, path.indexOf('/'));
		String folder = path.substring(path.indexOf('/') + 1);
		ObjectListing objs = client.listObjects(new ListObjectsRequest().withBucketName(bucket)
				.withPrefix(folder + '/'));
		return objs.getObjectSummaries() != null && !objs.getObjectSummaries().isEmpty();
	}

	boolean fileExists(String fullPath) {
		String path = removeS3(fullPath);
		String bucket = path.substring(0, path.indexOf('/'));
		String file = path.substring(path.indexOf('/') + 1);
		ObjectListing objs = client.listObjects(new ListObjectsRequest().withBucketName(bucket).withPrefix(file));
		return objs.getObjectSummaries() != null && !objs.getObjectSummaries().isEmpty();
	}

	boolean deleteFolder(String fullPath) {
		if (!folderExists(fullPath)) return false;
		String path = removeS3(fullPath);
		String bucket = path.substring(0, path.indexOf('/'));
		String folder = path.substring(path.indexOf('/') + 1);
		deleteFolderRec(bucket, folder);
		return true;
	}

	private void deleteFolderRec(String bucket, String folder) {
		List<S3ObjectSummary> l = client.listObjects(bucket, folder).getObjectSummaries();
		for (S3ObjectSummary f : l)
			client.deleteObject(bucket, f.getKey());
		client.deleteObject(bucket, folder);
	}

	boolean deleteFile(String fullPath) {
		if (!fileExists(fullPath)) return false;
		String path = removeS3(fullPath);
		String bucket = path.substring(0, path.indexOf('/'));
		String file = path.substring(path.indexOf('/') + 1);
		client.deleteObject(bucket, file);
		return true;
	}

	boolean uploadFile(String file, String fullPath) {
		File local = new File(file);
		if (!(local.exists() && local.canRead() && local.isFile())) return false;
		String folder = removeS3(fullPath);
		String bucket = folder;
		String name = "";
		if (folder.indexOf('/') > 0) { // Subfolder exists.
			bucket = folder.substring(0, folder.indexOf('/'));
			name = folder.substring(folder.indexOf('/') + 1) + '/';
		}
		String remote = name + local.getName();
		client.putObject(new PutObjectRequest(bucket, remote, local));
		return true;
	}

	private static String removeS3(String path) {
		if (!path.startsWith("s3://")) return path;
		return path.substring("s3://".length());
	}
}
