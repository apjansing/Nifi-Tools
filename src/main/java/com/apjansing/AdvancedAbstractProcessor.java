package com.apjansing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * @author apjansing
 *
 */
public abstract class AdvancedAbstractProcessor extends AbstractProcessor {
	
	final static Logger logger = LoggerFactory.getLogger( AdvancedAbstractProcessor.class );
	final static Gson gson = new Gson();
	
	/**
	 * @param flowFile to read from.
	 * @param session 
	 * @param charsetName of the encoding. Default is UTF-8 if null.
	 * @return a BufferedReader of the content.
	 * @throws IOException
	 */
	public BufferedReader readAsBufferedReader( FlowFile flowFile, ProcessSession session, String charsetName ) throws IOException {
		if(charsetName.equals(null)) 
			return readAsBufferedReader(flowFile, session);
		try( InputStream in = session.read( flowFile ) ){
			return new BufferedReader( new InputStreamReader(in, charsetName) );
		} catch ( IOException e ) {
			throw new IOException("IOException occurred while trying to make BufferedReader from FlowFile.", e);
		}
	}
	
	/**
	 * @param flowFile to read from.
	 * @param session 
	 * @return a BufferedReader of the content.
	 * @throws IOException
	 */
	public BufferedReader readAsBufferedReader( FlowFile flowFile, ProcessSession session ) throws IOException {
		return readAsBufferedReader( flowFile, session, "UTF-8");
	}

	/**
	 * @param flowFile to read from.
	 * @param session 
	 * @param cs of the encoding. Default is UTF-8 if null.
	 * @return a BufferedReader of the content.
	 * @throws IOException
	 */
	public BufferedReader readAsBufferedReader( FlowFile flowFile, ProcessSession session, Charset cs ) throws IOException {
		if(cs.equals(null))
			return readAsBufferedReader(flowFile, session);
		try( InputStream in = session.read( flowFile ) ){
			return new BufferedReader( new InputStreamReader(in, cs ) );
		} catch ( IOException e ) {
			throw new IOException("IOException occurred while trying to make BufferedReader from FlowFile.", e);
		}
	}
	
	/**
	 * @param flowFile
	 * @param session
	 * @param jsonElement
	 * @return
	 */
	public FlowFile writeFlowFile( FlowFile flowFile, ProcessSession session, JsonElement jsonElement ) {
		return writeFlowFile( flowFile, session, gson.toJson( jsonElement ) );
	}
	
	/**
	 * @param flowFile
	 * @param session
	 * @param jsonElement
	 * @return
	 */
	public FlowFile writeFlowFile( FlowFile flowFile, ProcessSession session, JsonObject jsonElement ) {
		return writeFlowFile( flowFile, session, jsonElement.toString() );
	}
	
	/**
	 * @param flowFile
	 * @param session
	 * @param string
	 * @return
	 */
	public FlowFile writeFlowFile( FlowFile flowFile, ProcessSession session, final String string ) {
		return session.write( flowFile, ( out ) -> {
				out.write( string.getBytes() );
		});
	}
	
	/**
	 * @return
	 */
	public Logger getSlf4jLogger() {
		return LoggerFactory.getLogger( this.getClass() );
	}
	
	/**
	 * @param context
	 * @param propertyDescriptors
	 * @return
	 */
	public boolean allPropertiesNotNull( ProcessContext context, PropertyDescriptor ... propertyDescriptors ) {
		for( PropertyDescriptor propertyDescriptor : propertyDescriptors ) {
			if( !propertyNotNull( context, propertyDescriptor ) ) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * @param context
	 * @param propertyDescriptors
	 * @return
	 */
	public boolean anyPropertyNotNull( ProcessContext context, PropertyDescriptor ... propertyDescriptors ) {
		for( PropertyDescriptor propertyDescriptor : propertyDescriptors ) {
			if( propertyNotNull( context, propertyDescriptor ) ) {
				return true;
			}
		}
		return false;
	}
	
	
	/**
	 * @param context
	 * @param propertyDescriptor
	 * @return
	 */
	public boolean propertyNotNull( ProcessContext context, PropertyDescriptor propertyDescriptor ) {
		return context.getProperty( propertyDescriptor ).isSet();
	}
	
	

}
