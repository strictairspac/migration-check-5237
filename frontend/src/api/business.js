import {api,post} from './client.js';
export const batches={list:()=>api('/batches'),save:(d,id)=>post('/batches'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const mappings={list:()=>api('/mappings'),save:(d,id)=>post('/mappings'+(id?'/'+id:''),d,id?'PUT':'POST')};
export const runs={list:()=>api('/runs'),run:d=>post('/runs',d),cancel:id=>post('/runs/'+id+'/cancel',{}),resume:id=>post('/runs/'+id+'/resume',{}),targets:id=>api('/runs/'+id+'/targets'),rejected:id=>api('/runs/'+id+'/rejected'),restore:(id,backup)=>post('/runs/'+id+'/restore',{backup})};
