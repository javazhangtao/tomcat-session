package com.plugins.tomcat.session.redis;


import java.io.IOException;

/**
 * Created by zhangtao on 2016/1/8.
 */
public interface Serializer {
    void setClassLoader(ClassLoader loader);

    byte[] attributesHashFrom(RedisSession session) throws IOException;
    byte[] serializeFrom(RedisSession session, SessionSerializationMetadata metadata) throws IOException;
    void deserializeInto(byte[] data, RedisSession session, SessionSerializationMetadata metadata) throws IOException, ClassNotFoundException;
}
