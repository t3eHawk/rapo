"""Contains built-in field descriptors."""

import dataclasses as dc
import sqlalchemy as sa


@dc.dataclass
class Field:
    """Represents some application fields stored in database tables."""

    field_name: str
    data_type: None

    def literal(self, value):
        return sa.literal(value).label(self.column_name)

    @property
    def column(self):
        return sa.Column(self.column_name, self.data_type)

    @property
    def null(self):
        return sa.null().cast(self.data_type).label(self.column_name)

    @property
    def column_name(self):
        return f'rapo_{self.field_name}'


PROCESS_ID = Field('process_id', sa.Integer)
RESULT_KEY = Field('result_key', sa.Integer)
RESULT_VALUE = Field('result_value', sa.String(100))
RESULT_TYPE = Field('result_type', sa.String(15))
DISCREPANCY_ID = Field('discrepancy_id', sa.String(4000))
DISCREPANCY_DESCRIPTION = Field('discrepancy_description', sa.String(4000))
