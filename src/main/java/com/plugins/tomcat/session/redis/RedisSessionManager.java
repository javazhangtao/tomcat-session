package com.plugins.tomcat.session.redis;

import org.apache.catalina.*;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.util.*;

/**
 * redis  tomcat session  工具类
 * redis存储session。key：sessionId(<byte[]>)
 * Created by zhangtao on 2016/1/12.
 */
public class RedisSessionManager extends ManagerBase implements Lifecycle {

    enum SessionPersistPolicy {
        DEFAULT,
        SAVE_ON_CHANGE,
        ALWAYS_SAVE_AFTER_REQUEST;
        static SessionPersistPolicy fromName(String name) {
            for (SessionPersistPolicy policy : SessionPersistPolicy.values()) {
                if (policy.name().equalsIgnoreCase(name)) {
                    return policy;
                }
            }
            throw new IllegalArgumentException("Invalid session persist policy [" + name + "]. Must be one of " + Arrays.asList(SessionPersistPolicy.values())+ ".");
        }
    }

    protected byte[] NULL_SESSION = "null".getBytes();
    private final Log log = LogFactory.getLog(RedisSessionManager.class);
    protected String hosts = null;
    protected String password = null;
    protected int timeout = Protocol.DEFAULT_TIMEOUT;
    protected String sentinelMaster = null;
    Set<String> sentinelSet = null;
    protected RedisSessionHandlerValve handlerValve;
    protected ThreadLocal<RedisSession> currentSession = new ThreadLocal<>();
    protected ThreadLocal<SessionSerializationMetadata> currentSessionSerializationMetadata = new ThreadLocal<>();
    protected ThreadLocal<String> currentSessionId = new ThreadLocal<>();
    protected ThreadLocal<Boolean> currentSessionIsPersisted = new ThreadLocal<>();
    protected Serializer serializer;
    protected static String name = "RedisSessionManager";
    protected String serializationStrategyClass = "com.plugins.tomcat.session.redis.JavaSerializer";
    protected EnumSet<SessionPersistPolicy> sessionPersistPoliciesSet = EnumSet.of(SessionPersistPolicy.DEFAULT);
    /**
     * The lifecycle event support for this component.
     */
    protected LifecycleSupport lifecycle = new LifecycleSupport(this);
    protected JedisCluster jedisCluster;



    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSerializationStrategyClass(String strategy) {
        this.serializationStrategyClass = strategy;
    }

    public String getSessionPersistPolicies() {
        StringBuilder policies = new StringBuilder();
        for (Iterator<SessionPersistPolicy> iter = this.sessionPersistPoliciesSet.iterator(); iter.hasNext();) {
            SessionPersistPolicy policy = iter.next();
            policies.append(policy.name());
            if (iter.hasNext()) {
                policies.append(",");
            }
        }
        return policies.toString();
    }

    public void setSessionPersistPolicies(String sessionPersistPolicies) {
        String[] policyArray = sessionPersistPolicies.split(",");
        EnumSet<SessionPersistPolicy> policySet = EnumSet.of(SessionPersistPolicy.DEFAULT);
        for (String policyName : policyArray) {
            SessionPersistPolicy policy = SessionPersistPolicy.fromName(policyName);
            policySet.add(policy);
        }
        this.sessionPersistPoliciesSet = policySet;
    }

    public boolean getSaveOnChange() {
        return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.SAVE_ON_CHANGE);
    }

    public boolean getAlwaysSaveAfterRequest() {
        return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.ALWAYS_SAVE_AFTER_REQUEST);
    }

    public String getSentinels() {
        StringBuilder sentinels = new StringBuilder();
        for (Iterator<String> iter = this.sentinelSet.iterator(); iter.hasNext();) {
            sentinels.append(iter.next());
            if (iter.hasNext()) {
                sentinels.append(",");
            }
        }
        return sentinels.toString();
    }

    public void setSentinels(String sentinels) {
        if (null == sentinels) {
            sentinels = "";
        }

        String[] sentinelArray = sentinels.split(",");
        this.sentinelSet = new HashSet<String>(Arrays.asList(sentinelArray));
    }

    public Set<String> getSentinelSet() {
        return this.sentinelSet;
    }

    public String getSentinelMaster() {
        return this.sentinelMaster;
    }

    public void setSentinelMaster(String master) {
        this.sentinelMaster = master;
    }

    public void setRejectedSessions(int i) {
        // Do nothing.
    }




    //================================================================================================================
    /**
     * 创建redis连接
     * @throws LifecycleException
     */
    private void initializeDatabaseConnection() throws LifecycleException {
        try {
            if(null==this.jedisCluster){
                String[] _hosts=getHosts().split(",");
                Set<HostAndPort> hap=new HashSet<HostAndPort>();
                for (String str:_hosts){
                    String[] hostAndPort=str.split(":");
                    if(null==hostAndPort || hostAndPort.length<2){
                        continue;
                    }
                    hap.add(new HostAndPort(hostAndPort[0], Integer.valueOf(hostAndPort[1])));
                }
                this.jedisCluster=new JedisCluster(hap,getTimeout());
            }
        } catch (Exception e) {
            log.error(e);
            throw new LifecycleException("Error connecting to RedisCluster", e);
        }
    }

    /**
     * 启动并初始化session管理相关上下文环境
     * Start this component and implement the requirements
     * of {@link org.apache.catalina.util.LifecycleBase#startInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error
     *  that prevents this component from being used
     */
    @Override
    protected synchronized void startInternal() throws LifecycleException {
        super.startInternal();
        setState(LifecycleState.STARTING);
        Boolean attachedToValve = false;
        for (Valve valve : getContainer().getPipeline().getValves()) {
            if (valve instanceof RedisSessionHandlerValve) {
                this.handlerValve = (RedisSessionHandlerValve) valve;
                this.handlerValve.setRedisSessionManager(this);
                log.info("Attached to RedisSessionHandlerValve");
                attachedToValve = true;
                break;
            }
        }
        if (!attachedToValve) {
            String error = "Unable to attach to session handling valve; sessions cannot be saved after the request without the valve starting properly.";
            log.fatal(error);
            throw new LifecycleException(error);
        }
        try {
            initializeSerializer();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.fatal("Unable to load serializer", e);
            throw new LifecycleException(e);
        }
        log.info("Will expire sessions after " + getMaxInactiveInterval() + " seconds");
        initializeDatabaseConnection();
        setDistributable(true);
    }


    /**
     * 停止tomcat manager
     * Stop this component and implement the requirements
     * of {@link org.apache.catalina.util.LifecycleBase#stopInternal()}.
     *
     * @exception LifecycleException if this component detects a fatal error
     *  that prevents this component from being used
     */
    @Override
    protected synchronized void stopInternal() throws LifecycleException {
        if (log.isDebugEnabled()) {
            log.debug("Stopping");
        }
        setState(LifecycleState.STOPPING);
        try {
            this.jedisCluster.close();
        } catch(Exception e) {}
        super.stopInternal();
    }

    /**
     * 删除session
     * @param session
     */
    @Override
    public void remove(Session session) {
        remove(session, false);
    }

    /**
     * redis删除session
     * @param session
     */
    @Override
    public void remove(Session session, boolean update) {
        Boolean error = true;
        log.trace("Removing session ID : " + session.getId());
        this.jedisCluster.del(session.getId().getBytes());
        error = false;
    }

    /**
     * 加载序列化工具类
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private void initializeSerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        log.info("Attempting to use serializer :" + serializationStrategyClass);
        serializer = (Serializer) Class.forName(serializationStrategyClass).newInstance();
        Loader loader = null;
        if (getContainer() != null) {
            loader = getContainer().getLoader();
        }
        ClassLoader classLoader = null;
        if (loader != null) {
            classLoader = loader.getClassLoader();
        }
        serializer.setClassLoader(classLoader);
    }

    /**
     * 创建新的session
     * @param requestedSessionId
     * @return
     */
    @Override
    public Session createSession(String requestedSessionId) {
        RedisSession session = null;
        String sessionId = null;
        String jvmRoute = getJvmRoute();
        Boolean error = true;


        // Ensure generation of a unique session identifier.
        if (null != requestedSessionId) {
            sessionId = sessionIdWithJvmRoute(requestedSessionId, jvmRoute);
            if (this.jedisCluster.setnx(sessionId.getBytes(), NULL_SESSION) == 0L) {
                sessionId = null;
            }
        } else {
            do {
                sessionId = sessionIdWithJvmRoute(generateSessionId(), jvmRoute);
            } while (this.jedisCluster.setnx(sessionId.getBytes(), NULL_SESSION) == 0L); // 1 = key set; 0 = key already existed
        }

        /* Even though the key is set in Redis, we are not going to flag
         the current thread as having had the session persisted since
         the session isn't actually serialized to Redis yet.
         This ensures that the save(session) at the end of the request
         will serialize the session into Redis with 'set' instead of 'setnx'. */

        error = false;

        if (null != sessionId) {
            session = (RedisSession)createEmptySession();
            session.setNew(true);
            session.setValid(true);
            session.setCreationTime(System.currentTimeMillis());
            session.setMaxInactiveInterval(getMaxInactiveInterval());
            session.setId(sessionId);
            session.tellNew();
        }
        currentSession.set(session);
        currentSessionId.set(sessionId);
        currentSessionIsPersisted.set(false);
        currentSessionSerializationMetadata.set(new SessionSerializationMetadata());
        if (null != session) {
            try {
                error = saveInternal(this.jedisCluster, session, true);
            } catch (IOException ex) {
                log.error("Error saving newly created session: " + ex.getMessage());
                currentSession.set(null);
                currentSessionId.set(null);
                session = null;
            }
        }

        return session;
    }


    /**
     * 根据jvm信息生成sessionId
     * @param sessionId
     * @param jvmRoute
     * @return
     */
    private String sessionIdWithJvmRoute(String sessionId, String jvmRoute) {
        if (jvmRoute != null) {
            String jvmRoutePrefix = '.' + jvmRoute;
            return sessionId.endsWith(jvmRoutePrefix) ? sessionId : sessionId + jvmRoutePrefix;
        }
        return sessionId;
    }

    /**
     * 创建新的session对象
     * @return
     */
    @Override
    public Session createEmptySession() {
        return new RedisSession(this);
    }

    /**
     * 添加session
     * @param session
     */
    @Override
    public void add(Session session) {
        try {
            save(session);
        } catch (IOException ex) {
            log.warn("Unable to add to session manager store: " + ex.getMessage());
            throw new RuntimeException("Unable to add to session manager store.", ex);
        }
    }

    /**
     * 根据当前sessionId查找redis中session对象
     * @param id
     * @return
     * @throws IOException
     */
    @Override
    public Session findSession(String id) throws IOException {
        RedisSession session = null;
        if (null == id) {
            currentSessionIsPersisted.set(false);
            currentSession.set(null);
            currentSessionSerializationMetadata.set(null);
            currentSessionId.set(null);
        } else if (id.equals(currentSessionId.get())) {
            session = currentSession.get();
        } else {
            byte[] data = loadSessionDataFromRedis(id);
            if (data != null) {
                DeserializedSessionContainer container = sessionFromSerializedData(id, data);
                session = container.session;

                //从属session从主session复制session数据(Attribute)===========================begin
                Object mapperId=session.getAttribute("mapperId");
                if(null!=mapperId){
                    byte[] mainData = loadSessionDataFromRedis(String.valueOf(mapperId));
                    if (mainData != null) {
                        DeserializedSessionContainer mainContainer = sessionFromSerializedData(String.valueOf(mapperId), mainData);
                        RedisSession mainSession=mainContainer.session;
                        Enumeration en = mainSession.getAttributeNames();
                        while(en.hasMoreElements()){
                            String key= (String) en.nextElement();
                            session.setAttribute(key,mainSession.getAttribute(key));
                        }
                    }
                }
                //从属session从主session复制session数据(Attribute)===========================end

                currentSession.set(session);
                currentSessionSerializationMetadata.set(container.metadata);
                currentSessionIsPersisted.set(true);
                currentSessionId.set(id);
            } else {
                currentSessionIsPersisted.set(false);
                currentSession.set(null);
                currentSessionSerializationMetadata.set(null);
                currentSessionId.set(null);
            }
        }

        return session;
    }

    /**
     * 根据sessionId获取redis中session对象byte[]数组
     * @param id
     * @return
     * @throws IOException
     */
    public byte[] loadSessionDataFromRedis(String id) throws IOException {
        Boolean error = true;
        log.trace("Attempting to load session " + id + " from Redis");
        byte[] data = this.jedisCluster.get(id.getBytes());
        error = false;
        if (data == null) {
            log.trace("Session " + id + " not found in Redis");
        }
        return data;
    }

    /**
     * session序列化容器
     * @param id
     * @param data
     * @return
     * @throws IOException
     */
    public DeserializedSessionContainer sessionFromSerializedData(String id, byte[] data) throws IOException {
        log.trace("Deserializing session " + id + " from Redis");
        if (Arrays.equals(NULL_SESSION, data)) {
            log.error("Encountered serialized session " + id + " with data equal to NULL_SESSION. This is a bug.");
            throw new IOException("Serialized session data was equal to NULL_SESSION");
        }
        RedisSession session = null;
        SessionSerializationMetadata metadata = new SessionSerializationMetadata();
        try {
            session = (RedisSession)createEmptySession();
            serializer.deserializeInto(data, session, metadata);
            session.setId(id);
            session.setNew(false);
            session.setMaxInactiveInterval(getMaxInactiveInterval());
            session.access();
            session.setValid(true);
            session.resetDirtyTracking();
        } catch (ClassNotFoundException ex) {
            log.fatal("Unable to deserialize into session", ex);
            throw new IOException("Unable to deserialize into session", ex);
        }
        return new DeserializedSessionContainer(session, metadata);
    }

    /**
     * 保存session
     * @param session
     * @throws IOException
     */
    public void save(Session session) throws IOException {
        save(session, false);
    }

    /**
     * 保存session进redis
     * @param session
     * @param forceSave
     * @throws IOException
     */
    public void save(Session session, boolean forceSave) throws IOException {
        Boolean error = true;
        try {
            error = saveInternal(this.jedisCluster, session, forceSave);
        } catch (IOException e) {
            throw e;
        }
    }

    /**
     * 保存session进redis并构建当前session对象
     * @param session
     * @param forceSave
     * @return
     * @throws IOException
     */
    protected boolean saveInternal(JedisCluster cluster, Session session, boolean forceSave) throws IOException {
        Boolean error = true;
        String mapperId = null;
        try {
            log.trace("Saving session " + session + " into Redis");
            RedisSession redisSession = (RedisSession)session;
            mapperId = String.valueOf(redisSession.getAttribute("mapperId"));
            byte[] binaryId = redisSession.getId().getBytes();
            Boolean isCurrentSessionPersisted;
            SessionSerializationMetadata sessionSerializationMetadata = currentSessionSerializationMetadata.get();
            byte[] originalSessionAttributesHash = sessionSerializationMetadata.getSessionAttributesHash();
            byte[] sessionAttributesHash = null;
            if (
                    forceSave
                            || redisSession.isDirty()
                            || null == (isCurrentSessionPersisted = this.currentSessionIsPersisted.get())
                            || !isCurrentSessionPersisted
                            || !Arrays.equals(originalSessionAttributesHash, (sessionAttributesHash = serializer.attributesHashFrom(redisSession)))
                    ) {
                log.trace("Save was determined to be necessary");

                if (null == sessionAttributesHash) {
                    sessionAttributesHash = serializer.attributesHashFrom(redisSession);
                }

                SessionSerializationMetadata updatedSerializationMetadata = new SessionSerializationMetadata();
                updatedSerializationMetadata.setSessionAttributesHash(sessionAttributesHash);

                cluster.set(binaryId, serializer.serializeFrom(redisSession, updatedSerializationMetadata));
                redisSession.resetDirtyTracking();
                currentSessionSerializationMetadata.set(updatedSerializationMetadata);
                currentSessionIsPersisted.set(true);
            } else {
                log.trace("Save was determined to be unnecessary");
            }
            log.trace("Setting expire timeout on session [" + redisSession.getId() + "] to " + getMaxInactiveInterval());
            cluster.expire(binaryId, getMaxInactiveInterval());

            if(null!=mapperId){
                byte[] data = loadSessionDataFromRedis(mapperId);
                if (data != null) {
                    redisSession.removeAttribute("mapperId");//主session对象删除mapperId数据
                    sessionAttributesHash = serializer.attributesHashFrom(redisSession);
                    if(null!=sessionAttributesHash){
                        SessionSerializationMetadata updatedSerializationMetadata = new SessionSerializationMetadata();
                        updatedSerializationMetadata.setSessionAttributesHash(sessionAttributesHash);
                        cluster.set(mapperId.getBytes(), serializer.serializeFrom(redisSession, updatedSerializationMetadata));
//                        redisSession.resetDirtyTracking();
                        cluster.expire(mapperId.getBytes(), getMaxInactiveInterval());
                    }
                }
            }

            error = false;
            return error;
        } catch (IOException e) {
            log.error(e.getMessage());
            throw e;
        } finally {
            return error;
        }
    }



    public void afterRequest() {
        RedisSession redisSession = currentSession.get();
        if (redisSession != null) {
            try {
                if (redisSession.isValid()) {
                    log.trace("Request with session completed, saving session " + redisSession.getId());
                    save(redisSession, getAlwaysSaveAfterRequest());
                } else {
                    log.trace("HTTP Session has been invalidated, removing :" + redisSession.getId());
                    remove(redisSession);
                }
            } catch (Exception e) {
                log.error("Error storing/removing session", e);
            } finally {
                currentSession.remove();
                currentSessionId.remove();
                currentSessionIsPersisted.remove();
                log.trace("Session removed from ThreadLocal :" + redisSession.getIdInternal());
            }
        }
    }



    //??????????????????????????????????????????????????????????????????????????????????????????????????????????????????

    /**
     * Get the lifecycle listeners associated with this lifecycle. If this
     * Lifecycle has no listeners registered, a zero-length array is returned.
     */
    @Override
    public LifecycleListener[] findLifecycleListeners() {
        return lifecycle.findLifecycleListeners();
    }

    /**
     * Remove a lifecycle event listener from this component.
     *
     * @param listener The listener to remove
     */
    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        lifecycle.removeLifecycleListener(listener);
    }

    @Override
    public int getRejectedSessions() {
        return 0;
    }

    @Override
    public void load() throws ClassNotFoundException, IOException {

    }

    @Override
    public void unload() throws IOException {

    }

    /**
     * Add a lifecycle event listener to this component.
     *
     * @param listener The listener to add
     */
    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        lifecycle.addLifecycleListener(listener);
    }

    @Override
    public void processExpires() {
        // We are going to use Redis's ability to expire keys for session expiration.
        // Do nothing.
    }
}

class DeserializedSessionContainer {
    public final RedisSession session;
    public final SessionSerializationMetadata metadata;
    public DeserializedSessionContainer(RedisSession session, SessionSerializationMetadata metadata) {
        this.session = session;
        this.metadata = metadata;
    }
}

