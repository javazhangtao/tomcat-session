package com.plugins.tomcat.session.redis;


import org.apache.catalina.util.CustomObjectInputStream;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.HashMap;

/**
 * Created by zhangtao on 2016/1/8.
 */
public class JavaSerializer implements Serializer {
    private ClassLoader loader;

    @Override
    public void setClassLoader(ClassLoader loader) {
        this.loader = loader;
    }

    public byte[] attributesHashFrom(RedisSession session) throws IOException {
        HashMap<String,Object> attributes = new HashMap<String,Object>();
        for (Enumeration<String> enumerator = session.getAttributeNames(); enumerator.hasMoreElements();) {
            String key = enumerator.nextElement();
            attributes.put(key, session.getAttribute(key));
        }

        byte[] serialized = null;

        try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos));
        ) {
            oos.writeUnshared(attributes);
            oos.flush();
            serialized = bos.toByteArray();
        }

        MessageDigest digester = null;
        try {
            digester = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
        }
        return digester.digest(serialized);
    }

    @Override
    public byte[] serializeFrom(RedisSession session, SessionSerializationMetadata metadata) throws IOException {
        byte[] serialized = null;
        try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos));
        ) {
            oos.writeObject(metadata);
            session.writeObjectData(oos);
            oos.flush();
            serialized = bos.toByteArray();
        }

        return serialized;
    }

    @Override
    public void deserializeInto(byte[] data, RedisSession session, SessionSerializationMetadata metadata) throws IOException, ClassNotFoundException {
        try(
                BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(data));
                ObjectInputStream ois = new CustomObjectInputStream(bis, loader);
        ) {
            SessionSerializationMetadata serializedMetadata = (SessionSerializationMetadata)ois.readObject();
            metadata.copyFieldsFrom(serializedMetadata);
            session.readObjectData(ois);
        }
    }
}
