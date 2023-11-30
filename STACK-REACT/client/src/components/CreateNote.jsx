import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import axios from "axios";

import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";

function CreateNote() {
  const [content, setContent] = useState({
    title: "",
    content: "",
    date: new Date(),
    userSelected: "",
    users: [],
    _id: "",
  });
  const [editing, setEditing] = useState(false);
  const params = useParams();
  const navigate = useNavigate();

  useEffect(() => {
    if (params.id) {
    }
  }, []);

  const onSubmit = async (e) => {
    e.preventDefault();
    if (editing) {
      const updatedNote = {
        title: this.state.title,
        content: this.state.content,
        author: this.state.userSelected,
        date: this.state.date,
      };
      await axios.put(
        "http://localhost:5002/api/notes/" + this.state._id,
        updatedNote
      );
    } else {
      const newNote = {
        title: content.title,
        content: content.content,
        author: 'Juanita M',
        date: content.date,
      };
      const res = await axios.post("http://localhost:5002/api/notes", newNote);
      console.log(res)
    }
  };

  const onInputChange = ({ target: { name, value } }) =>
    setContent({
      ...content,
      [name]: value,
    });

  const onChangeDate = (date) => setContent({ ...content, date });

  return (
    <div className="col-md-6 offset-md-3">
      <div className="card card-body">
        <h3><b>Create a Note</b></h3>
        <form onSubmit={onSubmit}>
          <div className="mb-3">
            <input
              type="text"
              className="form-control"
              placeholder="Title"
              onChange={onInputChange}
              name="title"
              value={content.title}
              required
              autoFocus
            />
          </div>

          {}
          <div className="mb-3">
            <textarea
              type="text"
              className="form-control"
              placeholder="Content"
              name="content"
              onChange={onInputChange}
              value={content.content}
              required
            ></textarea>
          </div>
          {}
          <div className="form-group">
            <DatePicker
              className="form-control"
              selected={content.date}
              onChange={onChangeDate}
            />
          </div>
          <button className="btn btn-primary">
            Save <i className="material-icons"></i>
          </button>
        </form>
      </div>
    </div>
  );
}

export default CreateNote;


