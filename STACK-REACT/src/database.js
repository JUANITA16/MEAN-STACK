import mongoose from "mongoose";

mongoose.set('strictQuery', false);

export const connectDB = async () => {
    try {
        await mongoose.connect("mongodb+srv://juanita16del2005:npK48rOLxmbWVyTo@juanita.rsra9ab.mongodb.net/", {
            useNewUrlParser: true,
            useUnifiedTopology: true
        });
        console.log(">> DB Connect");
    } catch (error) {
        console.error('Error connecting to database:', error);
    }
}