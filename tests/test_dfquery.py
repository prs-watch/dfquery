from unittest import TestCase
from dfquery import DFQuery
from pandas.testing import assert_frame_equal
import pandas as pd

# resources
DQ = DFQuery(globals())
DT = pd.DataFrame({"ID": [100, 200, 300], "AGE": [10, 20, 30]})
IT = pd.DataFrame({"ID": [100], "NAME": ["foo"]})

# answers
TEST_READ_ANSWER = pd.DataFrame({"ID": [200], "AGE": [20]})
TEST_READ_WITH_JOIN_ANSWER = pd.DataFrame({"ID": [100], "AGE": [10], "NAME": ["foo"]})
TEST_UPDATE_ANSWER = pd.DataFrame({"ID": [100, 200, 300], "AGE": [50, 50, 50]})
TEST_UPDATE_WITH_JOIN_ANSWER = pd.DataFrame(
    {"ID": [100, 200, 300], "AGE": [60, 20, 30]}
)


class TestDFQuery(TestCase):
    @classmethod
    def tearDownClass(cls):
        DQ.close()

    def test_read(self):
        assert_frame_equal(
            DQ.read(DT, "select * from DT where id = 200"), TEST_READ_ANSWER
        )

    def test_read_with_join(self):
        assert_frame_equal(
            DQ.read(
                DT,
                "select DT.ID, DT.AGE, IT.NAME from DT inner join IT on DT.ID = IT.ID",
                resources=[IT],
            ),
            TEST_READ_WITH_JOIN_ANSWER,
        )

    def test_update(self):
        assert_frame_equal(
            DQ.update(
                DT,
                "update DT set AGE = 50",
                resources=[IT],
            ),
            TEST_UPDATE_ANSWER,
        )

    def test_update_with_join(self):
        assert_frame_equal(
            DQ.update(
                DT,
                "update DT set AGE = 60 where ID = (select ID from IT)",
                resources=[IT],
            ),
            TEST_UPDATE_WITH_JOIN_ANSWER,
        )
