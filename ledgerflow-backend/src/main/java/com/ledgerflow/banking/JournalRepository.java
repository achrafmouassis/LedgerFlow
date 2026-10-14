package com.ledgerflow.banking;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;
import java.util.*;

public interface JournalRepository extends JpaRepository<Journal, UUID> {
    Optional<Journal> findByIdempotencyKey(String key);
    @Query("select j from Journal j where j.debitAccountId = :id or j.creditAccountId = :id order by j.createdAt desc, j.id desc")
    List<Journal> history(@Param("id") UUID id, Pageable pageable);
}
