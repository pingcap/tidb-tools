// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schemacmp_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"

	. "github.com/pingcap/tidb-tools/pkg/schemacmp"
)

type typeSchema struct{}

var _ = Suite(&typeSchema{})

const binary = "binary"

func init() {
	// INT
	typeInt = types.NewFieldType(mysql.TypeLong)
	typeInt.SetFlag(0)
	typeInt.SetFlen(11)
	typeInt.SetDecimal(0)
	typeInt.SetCharset(binary)
	typeInt.SetCollate(binary)

	// INT NOT NULL
	typeIntNotNull = types.NewFieldType(mysql.TypeLong)
	typeIntNotNull.SetFlag(mysql.NoDefaultValueFlag | mysql.NotNullFlag)
	typeIntNotNull.SetFlen(10)
	typeIntNotNull.SetDecimal(0)
	typeIntNotNull.SetCharset(binary)
	typeIntNotNull.SetCollate(binary)

	// INT AUTO_INCREMENT UNIQUE
	typeIntAutoIncrementUnique = types.NewFieldType(mysql.TypeLong)
	typeIntAutoIncrementUnique.SetFlag(mysql.AutoIncrementFlag | mysql.UniqueKeyFlag)
	typeIntAutoIncrementUnique.SetFlen(11)
	typeIntAutoIncrementUnique.SetDecimal(0)
	typeIntAutoIncrementUnique.SetCharset(binary)
	typeIntAutoIncrementUnique.SetCollate(binary)

	// INT NOT NULL, KEY
	typeIntNotNullKey = types.NewFieldType(mysql.TypeLong)
	typeIntNotNullKey.SetFlag(mysql.NoDefaultValueFlag | mysql.MultipleKeyFlag | mysql.NotNullFlag)
	typeIntNotNullKey.SetFlen(11)
	typeIntNotNullKey.SetDecimal(0)
	typeIntNotNullKey.SetCharset(binary)
	typeIntNotNullKey.SetCollate(binary)

	// INT(1)
	typeInt1 = types.NewFieldType(mysql.TypeLong)
	typeInt1.SetFlag(0)
	typeInt1.SetFlen(1)
	typeInt1.SetDecimal(0)
	typeInt1.SetCharset(binary)
	typeInt1.SetCollate(binary)

	// INT(22)
	typeInt22 = types.NewFieldType(mysql.TypeLong)
	typeInt22.SetFlag(0)
	typeInt22.SetFlen(22)
	typeInt22.SetDecimal(0)
	typeInt22.SetCharset(binary)
	typeInt22.SetCollate(binary)

	// BIT(4)
	typeBit4 = types.NewFieldType(mysql.TypeBit)
	typeBit4.SetFlag(mysql.UnsignedFlag)
	typeBit4.SetFlen(4)
	typeBit4.SetDecimal(0)
	typeBit4.SetCharset(binary)
	typeBit4.SetCollate(binary)

	// BIGINT(22) ZEROFILL
	typeBigInt22ZeroFill = types.NewFieldType(mysql.TypeLonglong)
	typeBigInt22ZeroFill.SetFlag(mysql.ZerofillFlag | mysql.UnsignedFlag)
	typeBigInt22ZeroFill.SetFlen(22)
	typeBigInt22ZeroFill.SetDecimal(0)
	typeBigInt22ZeroFill.SetCharset(binary)
	typeBigInt22ZeroFill.SetCollate(binary)

	// DECIMAL(16, 8) DEFAULT 2.5
	typeDecimal16_8 = types.NewFieldType(mysql.TypeNewDecimal)
	typeDecimal16_8.SetFlag(0)
	typeDecimal16_8.SetFlen(16)
	typeDecimal16_8.SetDecimal(8)
	typeDecimal16_8.SetCharset(binary)
	typeDecimal16_8.SetCollate(binary)

	// DECIMAL
	typeDecimal = types.NewFieldType(mysql.TypeNewDecimal)
	typeDecimal.SetFlag(0)
	typeDecimal.SetFlen(11)
	typeDecimal.SetDecimal(0)
	typeDecimal.SetCharset(binary)
	typeDecimal.SetCollate(binary)

	// Date
	typeDate = types.NewFieldType(mysql.TypeDate)
	typeDate.SetFlag(mysql.BinaryFlag)
	typeDate.SetFlen(10)
	typeDate.SetDecimal(0)
	typeDate.SetCharset(binary)
	typeDate.SetCollate(binary)

	// DATETIME(3)
	typeDateTime3 = types.NewFieldType(mysql.TypeDatetime)
	typeDateTime3.SetFlag(mysql.BinaryFlag)
	typeDateTime3.SetFlen(23)
	typeDateTime3.SetDecimal(3)
	typeDateTime3.SetCharset(binary)
	typeDateTime3.SetCollate(binary)

	// TIMESTAMP
	typeTimestamp = types.NewFieldType(mysql.TypeTimestamp)
	typeTimestamp.SetFlag(mysql.BinaryFlag)
	typeTimestamp.SetFlen(19)
	typeTimestamp.SetDecimal(0)
	typeTimestamp.SetCharset(binary)
	typeTimestamp.SetCollate(binary)

	// TIME(6)
	typeTime6 = types.NewFieldType(mysql.TypeDuration)
	typeTime6.SetFlag(mysql.BinaryFlag)
	typeTime6.SetFlen(17)
	typeTime6.SetDecimal(6)
	typeTime6.SetCharset(binary)
	typeTime6.SetCollate(binary)

	// YEAR(4)
	typeYear4 = types.NewFieldType(mysql.TypeYear)
	typeYear4.SetFlag(mysql.ZerofillFlag | mysql.UnsignedFlag)
	typeYear4.SetFlen(4)
	typeYear4.SetDecimal(0)
	typeYear4.SetCharset(binary)
	typeYear4.SetCollate(binary)

	// CHAR(123)
	typeChar123 = types.NewFieldType(mysql.TypeString)
	typeChar123.SetFlag(0)
	typeChar123.SetFlen(123)
	typeChar123.SetDecimal(0)
	typeChar123.SetCharset(mysql.UTF8MB4Charset)
	typeChar123.SetCollate(mysql.UTF8MB4DefaultCollation)

	// VARCHAR(65432) CHARSET ascii
	typeVarchar65432CharsetASCII = types.NewFieldType(mysql.TypeVarchar)
	typeVarchar65432CharsetASCII.SetFlag(0)
	typeVarchar65432CharsetASCII.SetFlen(65432)
	typeVarchar65432CharsetASCII.SetDecimal(0)
	typeVarchar65432CharsetASCII.SetCharset("ascii")
	typeVarchar65432CharsetASCII.SetCollate("ascii_bin")

	// BINARY(69)
	typeBinary69 = types.NewFieldType(mysql.TypeString)
	typeBinary69.SetFlag(mysql.BinaryFlag)
	typeBinary69.SetFlen(69)
	typeBinary69.SetDecimal(0)
	typeBinary69.SetCharset(binary)
	typeBinary69.SetCollate(binary)

	// VARBINARY(420)
	typeVarBinary420 = types.NewFieldType(mysql.TypeVarchar)
	typeVarBinary420.SetFlag(mysql.BinaryFlag)
	typeVarBinary420.SetFlen(420)
	typeVarBinary420.SetDecimal(0)
	typeVarBinary420.SetCharset(binary)
	typeVarBinary420.SetCollate(binary)

	// LONGBLOB
	typeLongBlob = types.NewFieldType(mysql.TypeLongBlob)
	typeLongBlob.SetFlag(mysql.BinaryFlag)
	typeLongBlob.SetFlen(0xffffffff)
	typeLongBlob.SetDecimal(0)
	typeLongBlob.SetCharset(binary)
	typeLongBlob.SetCollate(binary)

	// MEDIUMTEXT
	typeMediumText = types.NewFieldType(mysql.TypeMediumBlob)
	typeMediumText.SetFlag(0)
	typeMediumText.SetFlen(0xffffff)
	typeMediumText.SetDecimal(0)
	typeMediumText.SetCharset(mysql.UTF8MB4Charset)
	typeMediumText.SetCollate(mysql.UTF8MB4DefaultCollation)

	// ENUM('tidb', 'tikv', 'tiflash', 'golang', 'rust')
	typeEnum5 = types.NewFieldType(mysql.TypeEnum)
	typeEnum5.SetFlag(0)
	typeEnum5.SetFlen(types.UnspecifiedLength)
	typeEnum5.SetDecimal(0)
	typeEnum5.SetCharset(mysql.UTF8MB4Charset)
	typeEnum5.SetCollate(mysql.UTF8MB4DefaultCollation)
	typeEnum5.SetElems([]string{"tidb", "tikv", "tiflash", "golang", "rust"})

	// ENUM('tidb', 'tikv')
	typeEnum2 = types.NewFieldType(mysql.TypeEnum)
	typeEnum2.SetFlag(0)
	typeEnum2.SetFlen(types.UnspecifiedLength)
	typeEnum2.SetDecimal(0)
	typeEnum2.SetCharset(mysql.UTF8MB4Charset)
	typeEnum2.SetCollate(mysql.UTF8MB4DefaultCollation)
	typeEnum2.SetElems([]string{"tidb", "tikv"})

	// SET('tidb', 'tikv', 'tiflash', 'golang', 'rust')
	typeSet5 = types.NewFieldType(mysql.TypeSet)
	typeSet5.SetFlag(0)
	typeSet5.SetFlen(types.UnspecifiedLength)
	typeSet5.SetDecimal(0)
	typeSet5.SetCharset(mysql.UTF8MB4Charset)
	typeSet5.SetCollate(mysql.UTF8MB4DefaultCollation)
	typeSet5.SetElems([]string{"tidb", "tikv", "tiflash", "golang", "rust"})

	// SET('tidb', 'tikv')
	typeSet2 = types.NewFieldType(mysql.TypeSet)
	typeSet2.SetFlag(0)
	typeSet2.SetFlen(types.UnspecifiedLength)
	typeSet2.SetDecimal(0)
	typeSet2.SetCharset(mysql.UTF8MB4Charset)
	typeSet2.SetCollate(mysql.UTF8MB4DefaultCollation)
	typeSet2.SetElems([]string{"tidb", "tikv"})

	// JSON
	typeJSON = types.NewFieldType(mysql.TypeJSON)
	typeJSON.SetFlag(mysql.BinaryFlag)
	typeJSON.SetFlen(types.UnspecifiedLength)
	typeJSON.SetDecimal(0)
	typeJSON.SetCharset(binary)
	typeJSON.SetCollate(binary)

	// INT NOT NULL KEY <join> INT AUTO_INC UNIQUE = INT AUTO_INC KEY,
	typeLongIncrMultiKey = types.NewFieldType(mysql.TypeLong)
	typeLongIncrMultiKey.SetFlag(mysql.AutoIncrementFlag | mysql.MultipleKeyFlag)
	typeLongIncrMultiKey.SetFlen(11)
	typeLongIncrMultiKey.SetDecimal(0)
	typeLongIncrMultiKey.SetCharset(binary)
	typeLongIncrMultiKey.SetCollate(binary)
}

var (
	// INT
	typeInt *types.FieldType

	// INT NOT NULL
	typeIntNotNull *types.FieldType

	// INT AUTO_INCREMENT UNIQUE
	typeIntAutoIncrementUnique *types.FieldType

	// INT NOT NULL, KEY
	typeIntNotNullKey *types.FieldType

	// INT(1)
	typeInt1 *types.FieldType

	// INT(22)
	typeInt22 *types.FieldType

	// BIT(4)
	typeBit4 *types.FieldType

	// BIGINT(22) ZEROFILL
	typeBigInt22ZeroFill *types.FieldType

	// DECIMAL(16, 8) DEFAULT 2.5
	typeDecimal16_8 *types.FieldType

	// DECIMAL
	typeDecimal *types.FieldType

	// DATE
	typeDate *types.FieldType

	// DATETIME(3)
	typeDateTime3 *types.FieldType

	// TIMESTAMP
	typeTimestamp *types.FieldType

	// TIME(6)
	typeTime6 *types.FieldType

	// YEAR(4)
	typeYear4 *types.FieldType

	// CHAR(123)
	typeChar123 *types.FieldType

	// VARCHAR(65432) CHARSET ascii
	typeVarchar65432CharsetASCII *types.FieldType

	// BINARY(69)
	typeBinary69 *types.FieldType

	// VARBINARY(420)
	typeVarBinary420 *types.FieldType

	// LONGBLOB
	typeLongBlob *types.FieldType

	// MEDIUMTEXT
	typeMediumText *types.FieldType

	// ENUM('tidb', 'tikv', 'tiflash', 'golang', 'rust')
	typeEnum5 *types.FieldType

	// ENUM('tidb', 'tikv')
	typeEnum2 *types.FieldType

	// SET('tidb', 'tikv', 'tiflash', 'golang', 'rust')
	typeSet5 *types.FieldType

	// SET('tidb', 'tikv')
	typeSet2 *types.FieldType

	// JSON
	typeJSON *types.FieldType

	// INT NOT NULL KEY <join> INT AUTO_INC UNIQUE = INT AUTO_INC KEY,
	typeLongIncrMultiKey *types.FieldType
)

func (*typeSchema) TestTypeUnwrap(c *C) {
	testCases := []*types.FieldType{
		typeInt,
		typeIntNotNull,
		typeIntAutoIncrementUnique,
		typeIntNotNullKey,
		typeInt1,
		typeInt22,
		typeBit4,
		typeBigInt22ZeroFill,
		typeDecimal16_8,
		typeDecimal,
		typeDate,
		typeDateTime3,
		typeTimestamp,
		typeTime6,
		typeYear4,
		typeChar123,
		typeVarchar65432CharsetASCII,
		typeBinary69,
		typeVarBinary420,
		typeLongBlob,
		typeMediumText,
		typeEnum5,
		typeEnum2,
		typeSet5,
		typeSet2,
		typeJSON,
	}

	for _, tc := range testCases {
		assert := func(expected interface{}, checker Checker, args ...interface{}) {
			c.Assert(expected, checker, append(args, Commentf("tc = %s", tc))...)
		}
		t := Type(tc)
		assert(t.Unwrap(), DeepEquals, tc)
	}
}

func (*typeSchema) TestTypeCompareJoin(c *C) {
	testCases := []struct {
		a             *types.FieldType
		b             *types.FieldType
		compareResult int
		compareError  string
		join          *types.FieldType
		joinError     string
	}{
		{
			a:             typeInt,
			b:             typeInt22,
			compareResult: -1,
			join:          typeInt22,
		},
		{
			a:             typeInt1,
			b:             typeInt,
			compareResult: -1,
			join:          typeInt,
		},
		{
			a:             typeInt,
			b:             typeIntNotNull,
			compareResult: 1,
			join:          typeInt,
		},
		{
			// Cannot join DEFAULT NULL with AUTO_INCREMENT.
			a:            typeInt,
			b:            typeIntAutoIncrementUnique,
			compareError: `at tuple index \d+: distinct singletons.*`, // TODO: Improve error messages.
			joinError:    `at tuple index \d+: distinct singletons.*`,
		},
		{
			// INT NOT NULL <join> INT AUTO_INC UNIQUE = INT AUTO_INC,
			// but an AUTO_INC column must be defined with a key, so the join is invalid.
			a:            typeIntNotNull,
			b:            typeIntAutoIncrementUnique,
			compareError: `at tuple index \d+: combining contradicting orders.*`,
			joinError:    `auto type but not defined as a key`,
		},
		{
			// INT NOT NULL KEY <join> INT AUTO_INC UNIQUE = INT AUTO_INC KEY,
			a:            typeIntNotNullKey,
			b:            typeIntAutoIncrementUnique,
			compareError: `at tuple index \d+: combining contradicting orders.*`,
			join:         typeLongIncrMultiKey,
		},
		{
			// DECIMAL of different Flen/Decimal cannot be compared
			a:            typeDecimal16_8,
			b:            typeDecimal,
			compareError: `at tuple index \d+: distinct singletons.*`,
			joinError:    `at tuple index \d+: distinct singletons.*`,
		},
		{
			a:            typeVarchar65432CharsetASCII,
			b:            typeVarBinary420,
			compareError: `at tuple index \d+: distinct singletons.*`,
			joinError:    `at tuple index \d+: distinct singletons.*`,
		},
		{
			a:             typeEnum5,
			b:             typeEnum2,
			compareResult: 1,
			join:          typeEnum5,
		},
		{
			a:             typeSet2,
			b:             typeSet5,
			compareResult: -1,
			join:          typeSet5,
		},
		{
			a:            typeSet5,
			b:            typeEnum5,
			compareError: `at tuple index \d+: incompatible mysql type.*`,
			joinError:    `at tuple index \d+: incompatible mysql type.*`,
		},
	}

	for _, tc := range testCases {
		assert := func(expected interface{}, checker Checker, args ...interface{}) {
			args = append(args, Commentf("a = %v %#x, b = %v %#x", tc.a, tc.a.GetFlag(), tc.b, tc.b.GetFlag()))
			c.Assert(expected, checker, args...)
		}

		a := Type(tc.a)
		b := Type(tc.b)
		cmp, err := a.Compare(b)
		if len(tc.compareError) != 0 {
			if err == nil {
				c.Log(cmp)
			}
			assert(err, ErrorMatches, tc.compareError)
		} else {
			assert(err, IsNil)
			assert(cmp, Equals, tc.compareResult)
		}

		cmp, err = b.Compare(a)
		if len(tc.compareError) != 0 {
			assert(err, ErrorMatches, tc.compareError)
		} else {
			assert(err, IsNil)
			assert(cmp, Equals, -tc.compareResult)
		}

		wrappedJoin, err := a.Join(b)
		if len(tc.joinError) != 0 {
			assert(err, ErrorMatches, tc.joinError)
		} else {
			assert(err, IsNil)
			assert(wrappedJoin.Unwrap(), DeepEquals, tc.join)

			cmp, err = wrappedJoin.Compare(a)
			assert(err, IsNil)
			assert(cmp, GreaterEqual, 0)

			cmp, err = wrappedJoin.Compare(b)
			assert(err, IsNil)
			assert(cmp, GreaterEqual, 0)
		}
	}
}
