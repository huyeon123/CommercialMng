package com.example.project;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDate;
import java.util.List;

public interface UserRepository extends JpaRepository<User, Long> {
    List<User> findAllByUpdated(LocalDate updated);

    @Query(value = "select min(u.id) from User u")
    long findMinId();

    @Query(value = "select max(u.id) from User u")
    long findMaxId();
}
