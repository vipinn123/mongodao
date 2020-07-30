package com.spring.trade.entities;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ZerodhaTickMongoRepository extends MongoRepository<ZerodhaTick, Long> {

}
