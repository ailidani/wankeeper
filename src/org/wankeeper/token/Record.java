package org.wankeeper.token;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wankeeper.server.Config;

// Per token history record
public abstract class Record {

    protected static final Logger LOG = LoggerFactory.getLogger(Record.class);

    protected final LeaseType type = Config.instance().getLeaseType();

    abstract boolean hit(TokenType type, int from);
}
