import ImageUpload from "./ImageUpload.tsx";
import ProfileImageComponent from "./ProfileImageComponent.tsx";
import ProfileImageImperative from "./ProfileImageImperative.tsx";

function App() {
  return (
    <>
      <main className="max-w-6xl mx-auto px-3 py-16">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <div>
            <h2 className="text-xl font-semibold mb-4">Upload Image</h2>
            <ImageUpload />
          </div>
          <div>
            <h2>Profile Image - imperative way</h2>
            <ProfileImageImperative />
            <hr />
            <h2>Profile Image - component</h2>
            <ProfileImageComponent />
          </div>
        </div>
      </main>
    </>
  );
}

export default App;
