import axios from 'axios';

const api = axios.create({
    baseURL: '/api',
});

export const getState = () => api.get('/state');
export const addNode = () => api.post('/add_node');
export const putData = (key, value) => api.post('/put', { key, value });
export const getData = (key) => api.post('/get', { key });
export const leaveNode = (id) => api.post('/leave_node', { id });

export default api;
