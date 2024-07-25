#pydantic
from pydantic import  BaseModel
from pydantic import Field


# ----------------------------------------------------------------------------------------------
# Models
# ----------------------------------------------------------------------------------------------

class ResultsFile(BaseModel):
    file_id: str = Field(..., example="20220105121000999" )
    date_upload: str = Field(...)
    filename: str = Field(...)
    original_filename: str = Field(...)
    results_per_row: list = Field(...)
    upload_type: str = Field(...)
    user_upload: str = Field(default='')