package dbutil

import (
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

func (*testDBSuite) TestShowGrants(c *C) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mockGrants := []string{
		"GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION",
		"GRANT PROXY ON ''@'' TO 'root'@'localhost' WITH GRANT OPTION",
	}
	rows := sqlmock.NewRows([]string{"Grants for root@localhost"})
	for _, g := range mockGrants {
		rows.AddRow(g)
	}
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(rows)

	grants, err := ShowGrants(ctx, db, "", "")
	c.Assert(err, IsNil)
	c.Assert(grants, DeepEquals, mockGrants)

	if err := mock.ExpectationsWereMet(); err != nil {
		c.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func (*testDBSuite) TestShowGrantsWithRoles(c *C) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mockGrantsWithoutRoles := []string{
		"GRANT USAGE ON *.* TO `u1`@`localhost`",
		"GRANT `r1`@`%`,`r2`@`%` TO `u1`@`localhost`",
	}
	rows1 := sqlmock.NewRows([]string{"Grants for root@localhost"})
	for _, g := range mockGrantsWithoutRoles {
		rows1.AddRow(g)
	}
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(rows1)

	mockGrantsWithRoles := []string{
		"GRANT USAGE ON *.* TO `u1`@`localhost`",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON `db1`.* TO `u1`@`localhost`",
		"GRANT `r1`@`%`,`r2`@`%` TO `u1`@`localhost`",
	}
	rows2 := sqlmock.NewRows([]string{"Grants for root@localhost"})
	for _, g := range mockGrantsWithRoles {
		rows2.AddRow(g)
	}
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(rows2)

	grants, err := ShowGrants(ctx, db, "", "")
	c.Assert(err, IsNil)
	c.Assert(grants, DeepEquals, mockGrantsWithRoles)

	if err := mock.ExpectationsWereMet(); err != nil {
		c.Errorf("there were unfulfilled expectations: %s", err)
	}
}
