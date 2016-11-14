package com.ajansing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class NifiTools {

	public NifiTools() {
	}
	
	public JsonElement readAsJsonElement(FlowFile flowFile, ProcessSession session) {
		return new JsonParser().parse(readAsString(flowFile, session).replaceAll("\n", "").replaceAll("\t", ""));
	}
	
	public JsonObject readAsJson(FlowFile flowFile, ProcessSession session){
		return readAsJsonElement(flowFile, session).getAsJsonObject();
	}
	
	public JsonArray readAsJsonArray(FlowFile flowFile, ProcessSession session){
		return confirmJsonArray(readAsJsonElement(flowFile, session));
	}
		
	private JsonArray confirmJsonArray(JsonElement E) {
		return E.isJsonArray() ? E.getAsJsonArray() : JsonElementToArray(E);
	}

	private JsonArray JsonElementToArray(JsonElement E) {
		JsonArray ja = new JsonArray();
		ja.add(E);
		return ja;
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
		return toArray(readAsString(flowFile, session), deliminator, t);
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

	
	public FlowFile writeFlowFile(FlowFile flowFile, ProcessSession session, final String string) {
		return session.write(flowFile, new OutputStreamCallback() {
			@Override
			public void process(OutputStream out) throws IOException {
				out.write(string.getBytes());
			}
		});
	}
	
	public FlowFile writeFlowFile(FlowFile flowFile, ProcessSession session, JsonElement jsonElement) {
		return writeFlowFile(flowFile, session, jsonElement.getAsString());
	}

}
