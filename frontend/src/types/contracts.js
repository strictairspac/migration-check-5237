/** @typedef {{id:number,name:string,source_file:string,rows:object[]}} Batch */
/** @typedef {{id:number,name:string,batch_id:number,rules:object[],chunk_size:number}} Mapping */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'failed'|'cancelled',cancel_requested:boolean,progress:null|{chunk:number,processed:number,accepted:number,rejected:number,chunks:object[]},result:object|null,error:null|string}} Run */
/** 运行中断（running 失联 / failed）后可 POST /runs/{id}/resume 恢复原运行，从 progress.chunk 断点续迁；已确认块不重复转换。 */
export {};
