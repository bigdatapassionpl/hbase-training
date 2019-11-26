package com.bigdatapassion.hbase.api.dao;

import org.junit.Before;
import org.junit.Test;
import com.bigdatapassion.hadoop.data.model.users.User;
import com.bigdatapassion.hbase.api.loader.LoadUserData;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class UsersDaoTestExternalTest {

    private static final String EMAIL = "jan@kowalski.pl";
    private static final String FORENAME = "Jan";
    private static final String SURNAME = "Kowalski";
    private static final String PASSWORD = "k12l3iu12313;k";

    private UsersDao usersDao;

    @Before
    public void before() throws IOException {
        usersDao = new UsersDao();
        new LoadUserData().load();
    }

    @Test
    public void shouldSaveUser() throws Exception {
        //given
        User user = new User(FORENAME, SURNAME, EMAIL, PASSWORD);

        //when
        usersDao.save(user);

        //then
        User resultUser = usersDao.findByEmail(EMAIL);
        assertThat(resultUser).isNotNull();
        assertThat(resultUser.getEmail()).isEqualTo(EMAIL);
        assertThat(resultUser.getForename()).isEqualTo(FORENAME);
        assertThat(resultUser.getSurname()).isEqualTo(SURNAME);
        assertThat(resultUser.getPassword()).isEqualTo(PASSWORD);
    }

    @Test
    public void shouldFindAllUser() throws Exception {
        //given
        usersDao.save(FORENAME, SURNAME, "1", PASSWORD);
        usersDao.save(FORENAME, SURNAME, "2", PASSWORD);
        usersDao.save(FORENAME, SURNAME, "3", PASSWORD);

        //when
        List<User> users = usersDao.findAll();

        //then
        assertThat(users).isNotNull();
        assertThat(users.size()).isGreaterThan(3);
    }

}