import { City, RouteData } from '../types';

export const cities: City[] = [
  {
    id: 'boulder',
    name: 'Boulder',
    coordinates: [-105.2705, 40.0150],
    zoom: 13
  },
  {
    id: 'denver',
    name: 'Denver',
    coordinates: [-104.9903, 39.7392],
    zoom: 12
  },
  {
    id: 'fortCollins',
    name: 'Fort Collins',
    coordinates: [-105.0844, 40.5853],
    zoom: 12
  },
  {
    id: 'coloradoSprings',
    name: 'Colorado Springs',
    coordinates: [-104.8214, 38.8339],
    zoom: 12
  },
  {
    id: 'longmont',
    name: 'Longmont',
    coordinates: [-105.1019, 40.1672],
    zoom: 12
  }
];

export const routes: Record<string, RouteData[]> = {
  boulder: [
    {
      id: '1',
      name: 'Broadway',
      coordinates: [[-105.2797, 40.0138], [-105.2797, 40.0238]],
      congestionLevel: 'High'
    },
    {
      id: '2',
      name: 'Pearl Street',
      coordinates: [[-105.2897, 40.0176], [-105.2697, 40.0176]],
      congestionLevel: 'Medium'
    },
    {
      id: '3',
      name: 'Canyon Boulevard',
      coordinates: [[-105.2897, 40.0190], [-105.2697, 40.0190]],
      congestionLevel: 'Low'
    },
    {
      id: '4',
      name: 'Baseline Road',
      coordinates: [[-105.2897, 40.0000], [-105.2697, 40.0000]],
      congestionLevel: 'Medium'
    },
    {
      id: '5',
      name: '28th Street',
      coordinates: [[-105.2597, 40.0100], [-105.2597, 40.0200]],
      congestionLevel: 'Low'
    },
    {
      id: '6',
      name: 'Arapahoe Avenue',
      coordinates: [[-105.2750, 40.0150], [-105.2650, 40.0150]],
      congestionLevel: 'High'
    }
  ],
  denver: [
    {
      id: '1',
      name: 'Colfax Avenue',
      coordinates: [[-105.0097, 39.7403], [-104.9703, 39.7403]],
      congestionLevel: 'High'
    },
    {
      id: '2',
      name: 'Speer Boulevard',
      coordinates: [[-105.0097, 39.7303], [-104.9703, 39.7303]],
      congestionLevel: 'Medium'
    },
    {
      id: '3',
      name: '16th Street Mall',
      coordinates: [[-105.0097, 39.7503], [-104.9703, 39.7503]],
      congestionLevel: 'Low'
    },
    {
      id: '4',
      name: 'Federal Boulevard',
      coordinates: [[-105.0100, 39.7200], [-104.9800, 39.7200]],
      congestionLevel: 'High'
    },
    {
      id: '5',
      name: 'Lincoln Street',
      coordinates: [[-105.0000, 39.7350], [-104.9800, 39.7350]],
      congestionLevel: 'Medium'
    }
  ],
  fortCollins: [
    {
      id: '1',
      name: 'College Avenue',
      coordinates: [[-105.0844, 40.5853], [-105.0844, 40.5953]],
      congestionLevel: 'Medium'
    },
    {
      id: '2',
      name: 'Mulberry Street',
      coordinates: [[-105.0944, 40.5853], [-105.0744, 40.5853]],
      congestionLevel: 'Low'
    },
    {
      id: '3',
      name: 'Harmony Road',
      coordinates: [[-105.0944, 40.5753], [-105.0744, 40.5753]],
      congestionLevel: 'High'
    },
    {
      id: '4',
      name: 'Drake Road',
      coordinates: [[-105.0840, 40.5700], [-105.0640, 40.5700]],
      congestionLevel: 'Medium'
    }
  ],
  coloradoSprings: [
    {
      id: '1',
      name: 'Academy Boulevard',
      coordinates: [[-104.8214, 38.8339], [-104.8214, 38.8439]],
      congestionLevel: 'High'
    },
    {
      id: '2',
      name: 'Powers Boulevard',
      coordinates: [[-104.8314, 38.8339], [-104.8114, 38.8339]],
      congestionLevel: 'Medium'
    },
    {
      id: '3',
      name: 'Garden of the Gods Road',
      coordinates: [[-104.8314, 38.8239], [-104.8114, 38.8239]],
      congestionLevel: 'Low'
    },
    {
      id: '4',
      name: 'Nevada Avenue',
      coordinates: [[-104.8200, 38.8200], [-104.8000, 38.8200]],
      congestionLevel: 'Medium'
    }
  ],
  longmont: [
    {
      id: '1',
      name: 'Main Street',
      coordinates: [[-105.1019, 40.1672], [-105.1019, 40.1772]],
      congestionLevel: 'Medium'
    },
    {
      id: '2',
      name: 'Ken Pratt Boulevard',
      coordinates: [[-105.1119, 40.1672], [-105.0919, 40.1672]],
      congestionLevel: 'High'
    },
    {
      id: '3',
      name: 'Hover Street',
      coordinates: [[-105.1119, 40.1572], [-105.0919, 40.1572]],
      congestionLevel: 'Low'
    },
    {
      id: '4',
      name: 'Mountain View Avenue',
      coordinates: [[-105.1119, 40.1700], [-105.0919, 40.1700]],
      congestionLevel: 'Medium'
    }
  ]
};
