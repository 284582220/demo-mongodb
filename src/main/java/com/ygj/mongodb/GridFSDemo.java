package com.ygj.mongodb;

import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.async.client.gridfs.GridFSBucket;
import com.mongodb.async.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.*;


public class GridFSDemo {
    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("172.19.100.130", 27017);
        MongoDatabase database = mongoClient.getDatabase("test");
        GridFSBucket gridFSBucket = GridFSBuckets.create(database, "files");
        uploadFile(gridFSBucket);

//        deleteFile(gridFSBucket, "5bc7fb5556c6882ef0a22a8d");
    }

    private static void uploadFile(GridFSBucket gridFSBucket) {
        try {
            InputStream streamToUploadFrom = new FileInputStream(new File("D:\\jdk-8u121-windows-x64.exe"));
            // Create some custom options
            GridFSUploadOptions options = new GridFSUploadOptions()
                    .chunkSizeBytes(358400)
                    .metadata(new Document("type", "presentation"));
            ObjectId fileId = gridFSBucket.uploadFromStream("mongodb-tutorial", streamToUploadFrom, options);

            System.out.println(fileId);
        } catch (FileNotFoundException e) {
            // handle exception
        }
    }

    private static void downloadFile(GridFSBucket gridFSBucket) {
        String fileName = "mongodb-tutorial";
        //下载文件数据库中files.files的filename
        try {
            FileOutputStream streamToDownloadTo = new FileOutputStream("E:\\jdk-8u121-windows-x64.exe");
            gridFSBucket.downloadToStream(fileName, streamToDownloadTo);
            streamToDownloadTo.close();
            System.out.println(streamToDownloadTo.toString());
        } catch (IOException e) {
            // handle exception
        }

    }

    private static void deleteFile(GridFSBucket gridFSBucket, String objectIdStr){
        ObjectId fileId=new ObjectId(objectIdStr);
        //下载文件数据库中files.files的_Id
        gridFSBucket.delete(fileId);

    }


}
