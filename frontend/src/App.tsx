import { useState, useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AdminDashboard } from './pages/AdminDashboard';
import { Login } from './pages/Login';
import { NodeList } from './components/NodeList';
import { BucketList } from './components/BucketList';
import { ObjectBrowser } from './components/ObjectBrowser';
import {
  getNodes,
  getBuckets,
  createBucket,
  getObjects,
  uploadObject,
  deleteObject,
  type NodeStat,
  type Bucket,
  type ObjectInfo
} from './api';
import { LayoutDashboard, LogOut } from 'lucide-react';

type View = 'storage' | 'admin';

function StorageView() {
  const [view, setView] = useState<View>('storage');
  const [nodes, setNodes] = useState<NodeStat[]>([]);
  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
  const [objects, setObjects] = useState<ObjectInfo[]>([]);
  const [isUploading, setIsUploading] = useState(false);

  // Fetch initial data
  useEffect(() => {
    if (view === 'storage') {
      fetchNodes();
      fetchBuckets();
      const interval = setInterval(fetchNodes, 5000); // Poll nodes every 5s
      return () => clearInterval(interval);
    }
  }, [view]);

  // Fetch objects when bucket selected
  useEffect(() => {
    if (selectedBucket) {
      fetchObjects(selectedBucket);
    } else {
      setObjects([]);
    }
  }, [selectedBucket]);

  const fetchNodes = async () => {
    try {
      const data = await getNodes();
      setNodes(data);
    } catch (error) {
      console.error('Failed to fetch nodes', error);
    }
  };

  const fetchBuckets = async () => {
    try {
      const data = await getBuckets();
      setBuckets(data);
    } catch (error) {
      console.error('Failed to fetch buckets', error);
    }
  };

  const fetchObjects = async (bucket: string) => {
    try {
      const data = await getObjects(bucket);
      setObjects(data);
    } catch (error) {
      console.error('Failed to fetch objects', error);
    }
  };

  const handleCreateBucket = async (name: string) => {
    try {
      await createBucket(name);
      await fetchBuckets();
      setSelectedBucket(name);
    } catch (error) {
      alert('Failed to create bucket');
      console.error(error);
    }
  };

  const handleUpload = async (file: File) => {
    if (!selectedBucket) return;
    setIsUploading(true);
    try {
      await uploadObject(selectedBucket, file.name, file);
      await fetchObjects(selectedBucket);
      await fetchNodes(); // Update stats
    } catch (error) {
      alert('Failed to upload object');
      console.error(error);
    } finally {
      setIsUploading(false);
    }
  };

  const handleDelete = async (key: string) => {
    if (!selectedBucket) return;
    if (!confirm(`Delete ${key}?`)) return;
    try {
      await deleteObject(selectedBucket, key);
      await fetchObjects(selectedBucket);
      await fetchNodes(); // Update stats
    } catch (error) {
      alert('Failed to delete object');
      console.error(error);
    }
  };


  const handleLogout = () => {
    localStorage.removeItem('token');
    window.location.href = '/login';
  };

  if (view === 'admin') {
    return (
      <div className="min-h-screen bg-gray-100">
        <header className="bg-white shadow-sm border-b border-gray-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="bg-blue-600 p-1.5 rounded-lg">
                <LayoutDashboard className="w-6 h-6 text-white" />
              </div>
              <h1 className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-600 to-indigo-600">
                PlanetStore Dashboard
              </h1>
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => setView('storage')}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
              >
                Back to Storage
              </button>
              <button
                onClick={handleLogout}
                className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 flex items-center gap-2"
              >
                <LogOut className="w-4 h-4" />
                Logout
              </button>
            </div>
          </div>
        </header>
        <AdminDashboard />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-100 text-gray-900 font-sans">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="bg-blue-600 p-1.5 rounded-lg">
              <LayoutDashboard className="w-6 h-6 text-white" />
            </div>
            <h1 className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-600 to-indigo-600">
              PlanetStore Dashboard
            </h1>
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => setView('admin')}
              className="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700"
            >
              Admin Dashboard
            </button>
            <button
              onClick={handleLogout}
              className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 flex items-center gap-2"
            >
              <LogOut className="w-4 h-4" />
              Logout
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Node Stats */}
        <section>
          <h2 className="text-lg font-semibold mb-4 text-gray-700">Storage Nodes</h2>
          <NodeList nodes={nodes} />
        </section>

        {/* Main Content Area */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 h-[600px]">
          {/* Sidebar: Buckets */}
          <div className="md:col-span-1 h-full">
            <BucketList
              buckets={buckets}
              selectedBucket={selectedBucket}
              onSelectBucket={setSelectedBucket}
              onCreateBucket={handleCreateBucket}
            />
          </div>

          {/* Main: Object Browser */}
          <div className="md:col-span-3 h-full">
            {selectedBucket ? (
              <ObjectBrowser
                bucketName={selectedBucket}
                objects={objects}
                onUpload={handleUpload}
                onDelete={handleDelete}
                isUploading={isUploading}
              />
            ) : (
              <div className="bg-white rounded-lg shadow-md border border-gray-200 h-full flex items-center justify-center text-gray-400">
                Select a bucket to view objects
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}


function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    // Check authentication status
    const checkAuth = () => {
      const token = localStorage.getItem('token');
      console.log('Checking auth, token exists:', !!token);
      setIsAuthenticated(!!token);
      setIsLoading(false);
    };

    checkAuth();

    // Listen for storage changes (in case of login in another tab)
    window.addEventListener('storage', checkAuth);

    return () => window.removeEventListener('storage', checkAuth);
  }, []);

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-100">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading...</p>
        </div>
      </div>
    );
  }

  console.log('App render - isAuthenticated:', isAuthenticated);

  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={
          isAuthenticated ? <Navigate to="/" replace /> : <Login />
        } />
        <Route
          path="/"
          element={isAuthenticated ? <StorageView /> : <Navigate to="/login" replace />}
        />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
