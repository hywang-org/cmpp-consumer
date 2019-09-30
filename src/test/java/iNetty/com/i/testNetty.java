package iNetty.com.i;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Test;

public class testNetty {
    @Test
    public void step01_sendGet() {
        String result = requestGet("http://127.0.0.1:8080");
        System.out.println("ResultInfo = " + result);
    }

    @Test
    public void step01_sendPost() {
        String result = requestPost("http://127.0.0.1:8080");
        System.out.println("ResultInfo = " + result);
    }

    private String requestGet(String url) {
        String signature = null;
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        httpGet.setHeader("signature", signature);
        System.out.println("url = " + url);
        System.out.println("signature = " + signature);
        CloseableHttpResponse response = null;
        String responseString = null;
        try {
            response = client.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                String message = "call servie failed: " + response.getStatusLine();
                System.out.println(message);
            }
            // HttpEntity entity = response.getEntity();
            // byte[] responseContent =
            // IOUtils.toByteArray(entity.getContent());
            // responseString = IOUtils.toString(responseContent, "utf-8");
        } catch (IOException e) {
            System.out.println(e);
        } finally {
            IOUtils.closeQuietly(response);
        }
        return responseString;

    }

    private String requestPost(String url) {
        String signature = null;
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(url);
        httppost.setHeader("signature", signature);
        System.out.println("url = " + url);
        System.out.println("signature = " + signature);
        // HttpEntity reqEntity =
        // EntityBuilder.create().setBinary(uploadContent)
        // .setContentType(ContentType.create("application/json",
        // "UTF-8")).build();
        // httppost.setEntity(reqEntity);
        CloseableHttpResponse response = null;
        String responseString = null;
        try {
            response = client.execute(httppost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                String message = "call servie failed: " + response.getStatusLine();
                System.out.println(message);
            }
            HttpEntity entity = response.getEntity();
            byte[] responseContent = IOUtils.toByteArray(entity.getContent());
            responseString = IOUtils.toString(responseContent, "utf-8");
        } catch (IOException e) {
            System.out.println(e);
        } finally {
            IOUtils.closeQuietly(response);
        }
        return responseString;
    }

    private String requestPost2(String url, Map<String, Object> map, byte[] uploadContent) {
        String signature = null;
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(url);
        httppost.setHeader("signature", signature);
        System.out.println("url = " + url);
        System.out.println("signature = " + signature);
        HttpEntity reqEntity = EntityBuilder.create().setBinary(uploadContent)
                .setContentType(ContentType.create("application/json", "UTF-8")).build();
        httppost.setEntity(reqEntity);
        CloseableHttpResponse response = null;
        String responseString = null;
        try {
            response = client.execute(httppost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                String message = "call servie failed: " + response.getStatusLine();
                System.out.println(message);
            }
            HttpEntity entity = response.getEntity();
            byte[] responseContent = IOUtils.toByteArray(entity.getContent());
            responseString = IOUtils.toString(responseContent, "utf-8");
        } catch (IOException e) {
            System.out.println(e);
        } finally {
            IOUtils.closeQuietly(response);
        }
        return responseString;
    }
}
