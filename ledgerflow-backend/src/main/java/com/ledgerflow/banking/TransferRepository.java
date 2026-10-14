package com.ledgerflow.banking;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;
import java.util.*;

public interface TransferRepository extends JpaRepository<Transfer, UUID> {
    Optional<Transfer> findByIdempotencyKey(String key);
    @Query("select t from Transfer t where t.sourceAccountId = :id or t.destinationAccountId = :id order by t.createdAt desc, t.id desc")
    List<Transfer> history(@Param("id") UUID id, Pageable pageable);
}
