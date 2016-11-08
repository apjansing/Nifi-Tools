package com.ajansing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class NifiTools {

	public NifiTools() {
	}
	
	public JsonObject readAsJson(FlowFile flowFile, ProcessSession session){
		final Gson gson = new Gson();
		final StringBuilder builder = new StringBuilder();
		JsonParser jp = new JsonParser();
		session.read(flowFile, new InputStreamCallback() {
			@SuppressWarnings("deprecation")
			@Override
			public void process(InputStream in) throws IOException {
				builder.append(IOUtils.toString(in));
			}
		});
		return jp.parse(builder.toString().replaceAll("\n", "").replaceAll("\t", "")).getAsJsonObject();
	}
		
	public String readAsString(FlowFile flowFile, ProcessSession session){
		final StringBuilder builder = new StringBuilder();
		session.read(flowFile, new InputStreamCallback() {
			@SuppressWarnings("deprecation")
			@Override
			public void process(InputStream in) throws IOException {
				builder.append(IOUtils.toString(in));
			}
		});
		return builder.toString();
	}
	
	public <T> T[] readAsIntArray(FlowFile flowFile, ProcessSession session,  String deliminator, Class<T>[] t){
		final StringBuilder builder = new StringBuilder();
		session.read(flowFile, new InputStreamCallback() {
			@SuppressWarnings("deprecation")
			@Override
			public void process(InputStream in) throws IOException {
				builder.append(IOUtils.toString(in));
			}
		});
		return toArray(builder.toString(), deliminator, t);
	}
	
	@SuppressWarnings("unchecked")
	private <T> T[] toArray(String string, String deliminator, Class<T>[] clazz){
		String[] S = string.split(deliminator);
		T[] res = (T[]) Array.newInstance(clazz.getClass(), S.length);
		for(int i = 0; i < S.length; i++){
			res[i] = (T)S[i];
		}
		return res;
	}

	public FlowFile writeFlowFile(FlowFile flowFile, ProcessSession session, final JsonObject originalJson) {
		return session.write(flowFile, new OutputStreamCallback() {
			@Override
			public void process(OutputStream out) throws IOException {
				Gson gson = new Gson();
				out.write(gson.toJson(originalJson).getBytes());
			}
		});
	}
	
	public FlowFile writeFlowFile(FlowFile flowFile, ProcessSession session,  final String string) {
		return session.write(flowFile, new OutputStreamCallback() {
			@Override
			public void process(OutputStream out) throws IOException {
				Gson gson = new Gson();
				out.write(string.getBytes());
			}
		});
	}
	
}
