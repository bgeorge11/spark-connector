package com.marklogic.spark.java.connector;
import java.io.Serializable;

import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataReadHandle;

public class SparkDocument implements Serializable {

	private static final long serialVersionUID = 1L;
	String uri = "";
	AbstractWriteHandle content = null;
	DocumentMetadataReadHandle metaData = null;
	Format format = null;

	public SparkDocument(String uri, AbstractWriteHandle writeHandle, DocumentMetadataReadHandle metaHandle,
			Format format) {
		super();
		this.uri = uri;
		this.content = writeHandle;
		this.metaData = metaHandle;
		this.format = format;
	}

	public SparkDocument(DocumentRecord record) {
		super();
		AbstractWriteHandle writeHandle = null;
		if (record.getFormat().equals(Format.JSON)) {
			writeHandle = record.getContent(new JacksonHandle());
		}
		if (record.getFormat().equals(Format.XML)) {
			writeHandle = record.getContent(new DOMHandle());
		}
		if (record.getFormat().equals(Format.TEXT)) {
			writeHandle = record.getContent(new StringHandle());
		}
		uri = record.getUri();
		content = writeHandle;
		format = record.getFormat();
	}
	
	public Object getContentDocument() {
		if (format.equals(Format.XML)) {
			return (DOMHandle) content;
		}
		if (format.equals(Format.JSON)) {
			return (JacksonHandle) content;
		}
		if (format.equals(Format.TEXT)) {
			return (StringHandle) content;
		}
		return null;
	}
	
	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public AbstractWriteHandle getWriteHandle() {
		return content;
	}

	public void setWriteHandle(AbstractWriteHandle writeHandle) {
		this.content = writeHandle;
	}

	public DocumentMetadataReadHandle getMetaHandle() {
		return metaData;
	}

	public void setMetaHandle(DocumentMetadataReadHandle metaHandle) {
		this.metaData = metaHandle;
	}

	public Format getFormat() {
		return format;
	}

	public void setFormat(Format format) {
		this.format = format;
	}
}
