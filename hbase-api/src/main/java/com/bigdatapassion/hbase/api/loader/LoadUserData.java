package com.bigdatapassion.hbase.api.loader;

import com.bigdatapassion.hbase.api.dao.UsersDao;
import org.apache.hadoop.hbase.util.Bytes;
import com.bigdatapassion.hadoop.data.model.users.User;
import com.bigdatapassion.hbase.api.util.HBaseUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class LoadUserData extends HBaseLoader {

    @Override
    public void load() {
        try {
            HBaseUtil.recreateTable(UsersDao.TABLE, Bytes.toString(UsersDao.CF));

            UsersDao usersDao = new UsersDao();

            InputStream inputStream = LoadUserData.class.getClassLoader().getResourceAsStream("users/users.csv");

            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            String delimeter = ";";

            while ((line = br.readLine()) != null) {

                String[] userData = line.split(delimeter);

                User user = new User();
                user.setForename(userData[0]);
                user.setSurname(userData[1]);
                user.setEmail(userData[2]);
                user.setPassword(userData[3]);

                usersDao.save(user);
            }

            br.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
