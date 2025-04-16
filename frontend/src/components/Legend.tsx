import React from 'react';
import { AlertCircle } from 'lucide-react';

export function Legend() {
  return (
    <div className="absolute bottom-4 right-4 bg-white rounded-lg shadow-lg p-4 z-10">
      <div className="flex items-center gap-2 mb-3">
        <AlertCircle className="w-5 h-5 text-blue-600" />
        <h2 className="text-lg font-semibold text-gray-800">Congestion Levels</h2>
      </div>
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 rounded-full bg-red-500" />
          <span className="text-gray-700">High</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 rounded-full bg-yellow-500" />
          <span className="text-gray-700">Medium</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 rounded-full bg-green-500" />
          <span className="text-gray-700">Low</span>
        </div>
      </div>
    </div>
  );
}