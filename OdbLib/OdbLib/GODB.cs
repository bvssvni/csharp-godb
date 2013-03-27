/*

GODB - A self-maintained data base format.
BSD license.
by Sven Nilsen, 2013
http://www.cutoutpro.com
Version: 0.001 in angular degrees version notation
http://isprogrammingeasy.blogspot.no/2012/08/angular-degrees-versioning-notation.html

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
The views and conclusions contained in the software and documentation are those
of the authors and should not be interpreted as representing official policies,
either expressed or implied, of the FreeBSD Project.

*/

using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;

namespace OdbLib
{
	//
	// Idea and creation by Sven Nilsen.
	//
	// The ODB class is a way of handling multiple data
	// storing and handling.
	// Data is stored in object blocks and refered to by an OID.
	// This is the generics version.
	//
	public class GODB
	{
		private bool m_readOnly = false;

		// The filename to write or read from.
		// NOT USED: string m_filename;
		// The stream to file.
		FileStream m_stream;
		// The last OID used.
		long m_lastOID = 0;

		SortedList m_objectBlocks = new SortedList();
		SortedList m_deletedPos = new SortedList();
		public const int BLOCK_SIZE = 256;

		// Use 1 as root oid, because 0 is used to store OIDs.
		public const long ROOT_OID = 1;

		// Is raised when ODB want's to save changes.
		public event EventHandler SaveChanges;

		// Used to synchronize multiple access.
		private object m_dummy = new object();

		/// <summary>
		/// Returns true if file is empty.
		/// </summary>
		public bool IsEmpty
		{
			get
			{
				return (m_stream.Length == 0);
			}
		}

		public bool ReadOnly
		{
			get
			{return m_readOnly;}
		}

		public FileStream Stream
		{
			get
			{return m_stream;}
		}

		/// <summary>
		/// Generates a new OID.
		/// When requested, it increment the last OID variable.
		/// </summary>
		/// <returns>A new OID.</returns>
		public long GetNewOID()
		{
			m_lastOID++;
			if (m_lastOID == long.MaxValue)
			{
				m_lastOID = long.MinValue;
			}
			if (m_lastOID == 0)
			{
				// No more blocks, search manually.
				m_lastOID = -1;
				for (long i = 0; i < long.MaxValue; i++)
				{
					if (!Contains(i))
						return i;
				}
				for (long i = long.MinValue; i < -1; i++)
				{
					if (!Contains(i))
						return i;
				}
				throw new Exception("File limit exceeded");
			}
			return m_lastOID;
		}

		// Reserves an oid so it can't be used later.
		public void Reserve(long oid)
		{
			// Add a empty block for the reserved data.
			GObjectBlock reservedBlock = new GObjectBlock();
			reservedBlock.Blocks = new long[]{};
			reservedBlock.OID = oid;
			reservedBlock.CountBytes = 0;
			m_objectBlocks.Add(oid, reservedBlock);

			m_lastOID = Math.Max(oid, m_lastOID);
		}

		/// <summary>
		/// Check whether OID exists in ODB.
		/// </summary>
		/// <param name="oid"></param>
		/// <returns>Returns true if it contains OID.</returns>
		public bool Contains(long oid)
		{
			return this.m_objectBlocks.Contains(oid);
		}

		public GObjectBlock GetObjectBlock(long oid)
		{
			return (GObjectBlock)m_objectBlocks[oid];
		}

		// Create an empty object block,
		// prepared for stream writing.
		public GObjectBlock GetEmptyBlock(long oid)
		{
			GObjectBlock objectBlock = new GObjectBlock();
			objectBlock.OID = oid;
			objectBlock.CountBytes = 0;
			objectBlock.Blocks = new long[]{};
			this.m_objectBlocks.Add(oid, objectBlock);
			return objectBlock;
		}

		/// <summary>
		/// Reads bytes with specified OID.
		/// </summary>
		public byte[] Read(long oid)
		{
			if (!m_objectBlocks.Contains(oid))
				return null;
			GObjectBlock objectBlock = (GObjectBlock)m_objectBlocks[oid];
			byte[] result = new byte[objectBlock.CountBytes];
			for (int i = 0; i < objectBlock.Blocks.Length; i++)
			{
				// Goto block in file, and read it.
				m_stream.Seek(objectBlock.Blocks[i], System.IO.SeekOrigin.Begin);
				m_stream.Read(result, i * BLOCK_SIZE, Math.Min(BLOCK_SIZE, objectBlock.CountBytes - i * BLOCK_SIZE));
			}
			return result;
		}

		/// <summary>
		/// Saves bytes.
		/// If the OID writed has larger value than
		/// the automatically created OID, the next
		/// generated OID will follow the writed OID.
		/// </summary>
		/// <param name="oid">The OID (object identifier).</param>
		/// <param name="bytes">The bytes to write.</param>
		public void Write(long oid, byte[] bytes)
		{
			lock (m_dummy)
			{
				// Write block on new position.
				GObjectBlock objectBlock = new GObjectBlock();
				objectBlock.OID = oid;
				objectBlock.CountBytes = bytes.Length;

				// Delete old object block.
				Delete(oid);

				// Compute the number of blocks needed.
				int countBlocks = (int)Math.Ceiling(objectBlock.CountBytes / (double)BLOCK_SIZE);
				objectBlock.Blocks = this.FindNewPos(countBlocks);
				for (long i = 0; i < countBlocks; i++)
				{
					long pos = i * BLOCK_SIZE;
					m_stream.Seek(objectBlock.Blocks[i], System.IO.SeekOrigin.Begin);
					m_stream.Write(bytes, (int)pos, (int)(Math.Min(BLOCK_SIZE, bytes.Length - pos)));
				}

				// Add new object block.
				m_objectBlocks.Add(oid, objectBlock);

				if (oid > m_lastOID)
					m_lastOID = oid;
			}
		}

		private void ReserveOID(int bytesLength)
		{
			long oid = 0L;
			// Write block on new position.
			GObjectBlock objectBlock = new GObjectBlock();
			objectBlock.OID = oid;
			objectBlock.CountBytes = bytesLength;

			// Delete old object block.
			Delete(oid);

			// Compute the number of blocks needed.
			int countBlocks = (int)Math.Ceiling(objectBlock.CountBytes / (double)BLOCK_SIZE);
			objectBlock.Blocks = this.FindNewOIDPos(countBlocks);

			// Add new object block.
			m_objectBlocks.Add(oid, objectBlock);

			if (oid > m_lastOID)
				m_lastOID = oid;
		}

		// Find new positions to put the oid structure.
		public long[] FindNewOIDPos(int countBlocks)
		{
			lock (m_dummy)
			{
				if (countBlocks == 1 && this.m_deletedPos.Count > 0)
				{
					m_deletedPos.RemoveAt(0);
					// If only 1 block, we only need the first.
					return new long[]{0};
				}

				// Just put it where it is space.
				long[] pos = new long[countBlocks];
				int n = Math.Min(this.m_deletedPos.Count, countBlocks);
				for (int k = 0; k < n; k++)
				{
					pos[k] = (long)this.m_deletedPos.GetKey(0);
					m_deletedPos.Remove(pos[k]);
				}
				// (03.07.2007 09:01)
				// Make sure we start on the correct end.
				long streamLength = m_stream.Length;
				int totalBlocks = (int)Math.Ceiling(streamLength / (float)BLOCK_SIZE);
				long end = Math.Max(totalBlocks * (long)BLOCK_SIZE, BLOCK_SIZE);
				for (int k = n; k < countBlocks; k++)
					// subtract n to start at end.
					pos[k] = end + (k - n) * BLOCK_SIZE;

				// Delete all deleted positions that are after then end file.
				long lastPos = streamLength;
				int c = this.m_deletedPos.Count;
				long deletedpos;
				for (int i = c - 1; i >= 0; i--)
				{
					deletedpos = (long)this.m_deletedPos.GetByIndex(i);
					if (deletedpos >= lastPos)
						this.m_deletedPos.Remove(deletedpos);
					else
						break;
				}

				return pos;
			}
		}

		// Finds new positions that is after the specified position.
		public long[] FindNewPos(int countBlocks, long lastPos)
		{
			lock (m_dummy)
			{
				long[] positions = new long[countBlocks];
				long p;
				int n = m_deletedPos.Count;
				int j = 0;

				// Finds positions that is deleted 
				// and comes after the position.
				for (int i = 0; i < n && j < countBlocks; i++)
				{
					p = (long)this.m_deletedPos.GetByIndex(0);
					if (p > lastPos)
					{
						m_deletedPos.RemoveAt(i);
						n--;
						positions[j] = p;
						j++;
					}
				}

				if (j < countBlocks)
				{
					// (03.07.2007 09:01)
					// Make sure we are not using a block that is deleted.
					long streamLength = m_stream.Length;
					if (m_deletedPos.Count > 0)
						streamLength = Math.Max(streamLength, (long)m_deletedPos.GetByIndex(m_deletedPos.Count - 1) + BLOCK_SIZE);
					int totalBlocks = (int)Math.Ceiling(streamLength / (float)BLOCK_SIZE);
					long end = Math.Max(totalBlocks * (long)BLOCK_SIZE, BLOCK_SIZE);
					for (int k = 0; k < countBlocks - j; k++)
					{
						positions[j + k] = end + k * BLOCK_SIZE;
					}
				}
				return positions;
			}
		}

		public long[] FindNewPos(int countBlocks)
		{
			lock (m_dummy)
			{
				// If only 1 block, try the first and best.
				if (countBlocks == 1 && this.m_deletedPos.Count > 0)
				{
					long p = (long)this.m_deletedPos.GetByIndex(0);
					m_deletedPos.RemoveAt(0);
					return new long[]{p};
				}

				if (countBlocks > m_deletedPos.Count)
				{
					// Create new positions on end of file.
					long[] positions = new long[countBlocks];
					// (03.07.2007 09:01)
					// Make sure we start on the correct end.
					long streamLength = m_stream.Length;
					int totalBlocks = (int)Math.Ceiling(streamLength / (float)BLOCK_SIZE);
					long end = Math.Max(totalBlocks * (long)BLOCK_SIZE, BLOCK_SIZE);
					for (int k = 0; k < countBlocks; k++)
					{
						positions[k] = end + k * BLOCK_SIZE;
					}

					// Delete all deleted positions that are after then end file.
					long lastPos = streamLength;
					int c = this.m_deletedPos.Count;
					long deletedpos;
					for (int i = c - 1; i >= 0; i--)
					{
						deletedpos = (long)this.m_deletedPos.GetByIndex(i);
						if (deletedpos >= lastPos)
							this.m_deletedPos.Remove(deletedpos);
						else
							break;
					}

					return positions;
				}
				else if (countBlocks < m_deletedPos.Count)
				{
					// Try to find a room in deleted blocks that fits
					// the size we are looking for.
					int i = 1;
					int j = 1;
					long lastKey = -1;
					long startKey = -1;
					foreach (DictionaryEntry item in m_deletedPos)
					{
						if (lastKey != -1)
						{
							i += 1;
							if ((long)item.Key - lastKey > BLOCK_SIZE)
							{
								// Reset the number of blocks.
								i = 1;
								startKey = (long)item.Key;
							}
							else if (i >= countBlocks)
							{
								// We found a perfect match.
								long[] positions = new long[countBlocks];
								for (int k = 0; k < countBlocks; k++)
								{
									positions[k] = startKey + BLOCK_SIZE * k;
									m_deletedPos.Remove(positions[k]);
								}
								return positions;
							}
							else if (m_deletedPos.Count - j < countBlocks - i)
							{
								// Doesn't fit.
								break;
							}
						}
						else
						{
							startKey = (long)item.Key;
						}
						lastKey = (long)item.Key;
						j += 1;
					}
				}
			}

			// Just put it where it is space.
			long[] pos = new long[countBlocks];
			for (int k = 0; k < countBlocks; k++)
			{
				pos[k] = (long)this.m_deletedPos.GetKey(0);
				m_deletedPos.Remove(pos[k]);
			}

			return pos;
		}
		
		// Deletes object block.
		// Returns the object block that gets deleted.
		public GObjectBlock Delete(long oid)
		{
			lock (m_dummy)
			{
				// Mark old position as deleted.
				GObjectBlock oldObject = (GObjectBlock)this.m_objectBlocks[oid];
				if (oldObject != null)
				{
					foreach (long pos in oldObject.Blocks)
						m_deletedPos[pos] = pos;
					m_objectBlocks.Remove(oid);
				}
				return oldObject;
			}
		}

		// Deletes free blocks.
		public void DeleteBlocks(long[] blocks)
		{
			lock (m_dummy)
			{
				foreach (long pos in blocks)
				{
					if (!m_deletedPos.Contains(pos))
						m_deletedPos.Add(pos, pos);
				}
			}
		}

		private long m_oidPos = 0;
		private int m_oidPosIndex = 0;

		// (05.04.2007 10:19)
		// Saves oid internally in the file.
		public void SaveOIDs()
		{
			lock (m_dummy)
			{
				m_oidPos = 0;
				m_oidPosIndex = 0;

				if (SaveChanges != null)
					SaveChanges(this, new EventArgs());

				// Free the blocks of the structure, but
				// do not allow use of root block.
				Delete(0);
				m_deletedPos.RemoveAt(0);

				// (02.07.2007 09:01)
				// Remove all deleted blocks at the end of file.
				long lastPos = LastDataBlockPos();
				int c = this.m_deletedPos.Count;
				long deletedpos;
				for (int k = c - 1; k >= 0; k--)
				{
					deletedpos = (long)this.m_deletedPos.GetByIndex(k);
					if (deletedpos > lastPos)
						this.m_deletedPos.Remove(deletedpos);
					else
						break;
				}
				// (01.07.2007 13:43)
				// Set the length to be shortest as possible.
				this.m_stream.SetLength(lastPos + BLOCK_SIZE);
				
				// Make sure the file length ends at a block.
				long rest = m_stream.Length % BLOCK_SIZE;
				if (rest != 0)
					this.m_stream.SetLength(m_stream.Length + BLOCK_SIZE - m_stream.Length % BLOCK_SIZE);

				m_stream.Seek(0, System.IO.SeekOrigin.Begin);
				BinaryWriter w = new BinaryWriter(m_stream);

				// Repair the delete index if a block is in use
				// but marked as deleted.
				ICollection oids = m_objectBlocks.Values;
				foreach (GObjectBlock objectBlock in oids)
				{
					foreach (long pos in objectBlock.Blocks)
					{
						if (m_deletedPos.Contains(pos))
							m_deletedPos.Remove(pos);
					}
				}

				ArrayList oidBlocks = new ArrayList();
				oidBlocks.Add(0L);

				// Write number of object blocks.
				WriteOIDInt32(m_objectBlocks.Count, w, oidBlocks);
				int i = 0;
				foreach (GObjectBlock objectBlock in oids)
				{
					if (objectBlock.OID != 0)
					{
						/*
						// Stop if invalid object block.
						System.Diagnostics.Debug.Assert(objectBlock.OID != -1, "Invalid Object Block", "The OID of this Object Block is not set.");

						// Stop if the number of bytes is not correct.
						System.Diagnostics.Debug.Assert(objectBlock.CountBytes <= objectBlock.Blocks.Length * BLOCK_SIZE && objectBlock.CountBytes >= (objectBlock.Blocks.Length - 1) * BLOCK_SIZE, "Invalid object size", "The size of object in bytes is larger than\n the number of blocks multiplied the block size");
						*/
						
						WriteOIDInt64(objectBlock.OID, w, oidBlocks);
						WriteOIDInt32(objectBlock.CountBytes, w, oidBlocks);

						// Write block positions.
						WriteOIDInt32(objectBlock.Blocks.Length, w, oidBlocks);
						int j = 0;
						foreach (long pos in objectBlock.Blocks)
						{
							/*
							// Stop if invalid block pos.
							System.Diagnostics.Debug.Assert(pos - m_stream.Length <= BLOCK_SIZE * 4194304, "Invalid Block Position", "The block position was invalid because it points to a position 1 GB after the end of file.");
							*/
							
							WriteOIDInt64(pos, w, oidBlocks);
							j++;
						}

						i++;
					}
				}

				// Write number of deleted object blocks.
				WriteOIDInt32(this.m_deletedPos.Count, w, oidBlocks);
				ICollection positions = m_deletedPos.Values;
				long[] positionsTable = new long[positions.Count];
				positions.CopyTo(positionsTable, 0);
				foreach (long pos in positionsTable)
				{
					WriteOIDInt64(pos, w, oidBlocks);
				}

				GObjectBlock oidBlock = new GObjectBlock();
				oidBlock.Blocks = new long[oidBlocks.Count];
				oidBlocks.CopyTo(oidBlock.Blocks, 0);
				oidBlock.OID = 0L;
				oidBlock.CountBytes = oidBlock.Blocks.Length * BLOCK_SIZE;
				m_objectBlocks.Add(oidBlock.OID, oidBlock);
			}
		}

		private void WriteOIDInt32(int number, BinaryWriter w, ArrayList oidBlocks)
		{
			int bytesLeft = GODB.BLOCK_SIZE - (int)(this.m_stream.Position % GODB.BLOCK_SIZE);
			if (bytesLeft >= 4 + 8)
				w.Write(number);
			else
			{
				m_oidPosIndex++;
				// Use blocks that are deleted directly.
				if (m_deletedPos.Count > 0)
				{
					m_oidPos = (long)m_deletedPos.GetByIndex(0);
					m_deletedPos.Remove(m_oidPos);
					w.Write(m_oidPos);
				}
				else if (m_stream.Position == m_stream.Length)
				{
					// Since we are expanding the stream,
					// we need the position to the following block.
					m_oidPos = m_stream.Position + 8;
					m_stream.SetLength(m_oidPos + BLOCK_SIZE);
					w.Write(m_oidPos);
				}
				else
				{
					// Jump to the end of file.
					m_oidPos = m_stream.Length;
					// Hold the stream size in blocks.
					m_stream.SetLength(m_oidPos + BLOCK_SIZE);
					w.Write(m_oidPos);
				}
				oidBlocks.Add(m_oidPos);
				m_stream.Seek(m_oidPos, System.IO.SeekOrigin.Begin);
				w.Write(number);
			}
		}

		private void WriteOIDInt64(long number, BinaryWriter w, ArrayList oidBlocks)
		{
			int bytesLeft = GODB.BLOCK_SIZE - (int)(this.m_stream.Position % GODB.BLOCK_SIZE);
			if (bytesLeft >= 8 + 8)
				w.Write(number);
			else
			{
				m_oidPosIndex++;
				// Use blocks that are deleted directly.
				if (m_deletedPos.Count > 0)
				{
					m_oidPos = (long)m_deletedPos.GetByIndex(0);
					m_deletedPos.Remove(m_oidPos);
					w.Write(m_oidPos);
				}
				else if (m_stream.Position == m_stream.Length)
				{
					// Since we are expanding the stream,
					// we need the position to the following block.
					m_oidPos = m_stream.Position + 8;
					m_stream.SetLength(m_oidPos + BLOCK_SIZE);
					w.Write(m_oidPos);
				}
				else
				{
					// Jump to the end of file.
					m_oidPos = m_stream.Length;
					// Hold the stream size in blocks.
					m_stream.SetLength(m_oidPos + BLOCK_SIZE);
					w.Write(m_oidPos);
				}
				oidBlocks.Add(m_oidPos);
				m_stream.Seek(m_oidPos, System.IO.SeekOrigin.Begin);
				w.Write(number);
			}
		}

		private int ComputeOIDBlockSize()
		{
			int sum = 0;

			// The number of deleted block positions.
			sum += 4;

			// Add the positions of old oid block.
			sum += m_deletedPos.Count * 8;

			// Write number of object blocks.
			sum += 4;
			ICollection oids = m_objectBlocks.Values;
			foreach (GObjectBlock objectBlock in oids)
			{
				if (objectBlock.OID != 0)
				{
					// oid.
					sum += 8;
					// bytes.
					sum += 4;
					// blocks.
					sum += 4;

					// Positions.
					sum += 8 * objectBlock.Blocks.Length;
				}
			}

			// Add bytes for pointing to the next block.
			int blocks = (int)Math.Ceiling(sum / (float)(GODB.BLOCK_SIZE - 8));
			sum += blocks * 8;
			// Add a extra oid block because something always go wrong anyway.
			sum += GODB.BLOCK_SIZE;
			return sum;
		}
		// (05.04.2007 10:19)
		// Reads oids internally in the file.
		public void ReadOIDs()
		{
			m_oidPos = 0;
			// Make sure the file length ends at a block.
			long rest = m_stream.Length % BLOCK_SIZE;
			if (rest != 0)
				this.m_stream.SetLength(m_stream.Length + BLOCK_SIZE - m_stream.Length % BLOCK_SIZE);
			this.m_stream.Seek(0, System.IO.SeekOrigin.Begin);
			this.m_objectBlocks.Clear();
			this.m_deletedPos.Clear();

			GObjectBlock oidBlock = new GObjectBlock();
			if (m_stream.Length == 0)
			{
				oidBlock.OID = 0L;
				oidBlock.CountBytes = BLOCK_SIZE;
				oidBlock.Blocks = new long[]{0};
				m_objectBlocks.Add(0L, oidBlock);
				return;
			}

			BinaryReader r = new BinaryReader(m_stream);
			ArrayList oidBlocks = new ArrayList();
			oidBlocks.Add(0L);

			int countObjectBlocks = ReadOIDInt32(r, oidBlocks);
			long readPos = 0;
			for (int i = 0; i < countObjectBlocks; i++)
			{
				GObjectBlock objectBlock = new GObjectBlock();
				objectBlock.OID = ReadOIDInt64(r, oidBlocks);
				if (objectBlock.OID != 0)
				{
					if (objectBlock.OID > m_lastOID)
						m_lastOID = objectBlock.OID;
					objectBlock.CountBytes = ReadOIDInt32(r, oidBlocks);
				
					// Read block positions.
					int blockCount = ReadOIDInt32(r, oidBlocks);

					// Stop if the number of bytes is not correct.
					System.Diagnostics.Debug.Assert(objectBlock.CountBytes <= blockCount * BLOCK_SIZE && objectBlock.CountBytes >= (blockCount - 1) * BLOCK_SIZE, "Invalid object size", "The size of object in bytes is larger than\n the number of blocks multiplied the block size");

					objectBlock.Blocks = new long[blockCount];

					for (int j = 0; j < blockCount; j++)
					{
						readPos = ReadOIDInt64(r, oidBlocks);

						// Repear delete index if the block is used.
						if (this.m_deletedPos.Contains(readPos))
							this.m_deletedPos.Remove(readPos);

						objectBlock.Blocks[j] = readPos;
					}

					// Add object block.
					m_objectBlocks.Add(objectBlock.OID, objectBlock);
				}
			}

			// Read deleted block positions.
			int deletedBlocks = ReadOIDInt32(r, oidBlocks);
			for (int i = 0; i < deletedBlocks; i++)
			{
				long pos = ReadOIDInt64(r, oidBlocks);
				if (!m_deletedPos.Contains(pos))
					m_deletedPos.Add(pos, pos);
			}

			oidBlock.Blocks = new long[oidBlocks.Count];
			oidBlocks.CopyTo(oidBlock.Blocks, 0);
			oidBlock.OID = 0L;
			oidBlock.CountBytes = oidBlock.Blocks.Length * BLOCK_SIZE;
			m_objectBlocks.Add(oidBlock.OID, oidBlock);
		}

		// Reads an int in OID basis.
		private int ReadOIDInt32(BinaryReader r, ArrayList oidBlocks)
		{
			int bytesLeft = GODB.BLOCK_SIZE - (int)(this.m_stream.Position % GODB.BLOCK_SIZE);
			if (bytesLeft >= 4 + 8)
				return r.ReadInt32();
			else
			{
				long oldOidPos = m_oidPos;
				m_oidPos = r.ReadInt64();
				// Repair the oid position by using the block after it..
				if (m_oidPos < 0 || m_oidPos > m_stream.Length)
					m_oidPos = oldOidPos + BLOCK_SIZE;
				oidBlocks.Add(m_oidPos);

				// Stop if invalid block pointer.
				System.Diagnostics.Debug.Assert(m_oidPos < m_stream.Length && m_oidPos >= 0, "Invalid block pointer", "The next block is beyond the file size");

				m_stream.Seek(m_oidPos, System.IO.SeekOrigin.Begin);
				return r.ReadInt32();
			}
		}

		// Reads a long in OID basis.
		private long ReadOIDInt64(BinaryReader r, ArrayList oidBlocks)
		{
			int bytesLeft = GODB.BLOCK_SIZE - (int)(this.m_stream.Position % GODB.BLOCK_SIZE);
			if (bytesLeft >= 8 + 8)
				return r.ReadInt64();
			else
			{
				long oldOidPos = m_oidPos;
				m_oidPos = r.ReadInt64();
				// Repair the oid position by using the block after it..
				if (m_oidPos < oldOidPos || m_oidPos > m_stream.Length)
					m_oidPos = oldOidPos + BLOCK_SIZE;
				oidBlocks.Add(m_oidPos);

				// Stop if invalid block pointer.
				System.Diagnostics.Debug.Assert(m_oidPos < m_stream.Length && m_oidPos >= 0, "Invalid block pointer", "The next block is beyond the file size");

				m_stream.Seek(m_oidPos, System.IO.SeekOrigin.Begin);
				return r.ReadInt64();
			}
		}

		public GODB(bool readOnly)
		{
			m_readOnly = readOnly;
		}

        public void Open(string filename)
        { 
			// NOT USED: m_filename = filename;
			if (m_readOnly)
				m_stream = new FileStream(filename, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read);
			else
				m_stream = new FileStream(filename, System.IO.FileMode.OpenOrCreate, 
					System.IO.FileAccess.ReadWrite);
			this.ReadOIDs();
        }

        /// <summary>
        /// Closes the connection with file.
        /// </summary>
        public void Close()
        {
			if (!m_readOnly)
			{
				SaveOIDs();
			}

            m_stream.Close();
        }

		//
		// Returns the last position of data block.
		// The OID index is not counted with.
		//
		private long LastDataBlockPos()
		{
			long maxPos = long.MinValue;
			ICollection oids = this.m_objectBlocks.Values;
			foreach (GObjectBlock objectBlock in oids)
			{
				if (objectBlock.Blocks.Length > 0)
				{
					long pos = objectBlock.Blocks[objectBlock.Blocks.Length - 1];
					if (pos > maxPos)
						maxPos = pos;
				}
			}
			if (maxPos > 0)
				return maxPos;
			else
				return 0;
		}
	}
	
	
	/// <summary>
	/// GObjectBlock is used to link multiple byte blocks
	/// to describe one object.
	/// </summary>
	public class GObjectBlock : IComparable
	{
		long m_oid = -1;
		int m_countBytes;
		long[] m_blocks;

		/// <summary>
		/// Gets or sets the OID.
		/// </summary>
		public long OID
		{
			get
			{
				return m_oid;
			}
			set
			{
				m_oid = value;
			}
		}

		/// <summary>
		/// Gets or sets the length of object data.
		/// </summary>
		public int CountBytes
		{
			get
			{
				return m_countBytes;
			}
			set
			{
				m_countBytes = value;
			}
		}

		/// <summary>
		/// Gets or sets the file positions of blocks.
		/// </summary>
		public long[] Blocks
		{
			get
			{
				return m_blocks;
			}
			set
			{
				m_blocks = value;
			}
		}

		/// <summary>
		/// Compare an object block with another object block.
		/// </summary>
		/// <param name="obj">The object block to compare with.</param>
		/// <returns>Returns 1 if obj's OID is larger than this object block's OID.
		/// Returns -1 if obj's OID is smaller than this object block's OID.
		/// Return 0 if obj's OID equals this object block's OID.
		///</returns>
		public int CompareTo(object obj)
		{
			if (obj is GObjectBlock)
			{
				GObjectBlock objBlock = (GObjectBlock)obj;
				if (objBlock.m_oid < m_oid)
					return 1;
				else if (objBlock.m_oid > m_oid)
					return -1;
				else
					return 0;
			}
			return 0;
		}

		public GObjectBlock()
		{
		}
	}
	
	
	public class OdbStream : Stream
	{
		private GODB m_odb;
		private GObjectBlock m_objectBlock;
		private long m_position = 0;

		public OdbStream(GODB odb, long oid)
		{
			m_odb = odb;
			if (!m_odb.ReadOnly && !m_odb.Contains(oid))
				m_objectBlock = m_odb.GetEmptyBlock(oid);
			else
				m_objectBlock = m_odb.GetObjectBlock(oid);
		}

		private long ComputeOdbPos(long position)
		{
			// First, find the block number by dividing with the block size.
			// Then add the modulus position to get it within that block.
			long block = position / GODB.BLOCK_SIZE;
			long blockPos = position % GODB.BLOCK_SIZE;
			if (block >= m_objectBlock.Blocks.Length)
				return -1;
			return m_objectBlock.Blocks[block] + blockPos;
		}

		public override bool CanRead
		{
			get
			{return true;}
		}

		public override bool CanSeek
		{
			get
			{return true;}
		}

		public override bool CanWrite
		{
			get
			{
				return !m_odb.ReadOnly;
			}
		}

		public override void Flush()
		{
			m_odb.Stream.Flush();
		}

		public override long Length
		{
			get
			{
				return m_objectBlock.CountBytes;
			}
		}

		public override long Position
		{
			get
			{
				return m_position;
			}
			set
			{
				m_position = value;
				m_odb.Stream.Seek(this.ComputeOdbPos(value), System.IO.SeekOrigin.Begin);
			}
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			long odbPos = this.ComputeOdbPos(m_position);
			// Return if reached the end of the object blocks.
			if (odbPos == -1)
				return 0;
			m_odb.Stream.Seek(odbPos, System.IO.SeekOrigin.Begin);

			count = (int)Math.Min(m_objectBlock.CountBytes - m_position, count);

			long currentPos = m_position;
			long endPos = m_position + count;

			int remaining = count;
			long block = currentPos / GODB.BLOCK_SIZE;
			long blockPos = currentPos % GODB.BLOCK_SIZE;

			long blockEnd = endPos / GODB.BLOCK_SIZE;
			long blockEndPos = endPos % GODB.BLOCK_SIZE;

			// Read the first block.
			int firstBlockCount = GODB.BLOCK_SIZE;
			if (blockEnd == block)
				firstBlockCount = (int)(blockEndPos - blockPos);
			m_odb.Stream.Read(buffer, offset, firstBlockCount);
			remaining -= firstBlockCount;
			offset += firstBlockCount;

			// Read block in betweens.
			for (long i = block + 1; i < blockEnd; i++)
			{
				m_odb.Stream.Seek(m_objectBlock.Blocks[i], System.IO.SeekOrigin.Begin);
				m_odb.Stream.Read(buffer, offset, GODB.BLOCK_SIZE);
				offset += GODB.BLOCK_SIZE;
				remaining -= GODB.BLOCK_SIZE;
			}

			// Read end block.
			if (remaining > 0)
			{
				m_odb.Stream.Seek(m_objectBlock.Blocks[blockEnd], System.IO.SeekOrigin.Begin);
				m_odb.Stream.Read(buffer, offset, remaining);
			}

			m_position += count;

			return count;
		}

		// Do no actual seeking.
		// Helps multiple stream to read from ODB in same time.
		public override long Seek(long offset, SeekOrigin origin)
		{
			if (origin == System.IO.SeekOrigin.Begin)
			{
				m_position = offset;
			}
			else if (origin == System.IO.SeekOrigin.Current)
			{
				m_position = m_position + offset;
			}
			else if (origin == System.IO.SeekOrigin.End)
			{
				m_position = m_objectBlock.CountBytes - offset;
			}

			if (m_position < 0)
			{
				m_position = 0;
				throw new System.IO.EndOfStreamException();
			}
			else if (m_position > m_objectBlock.CountBytes)
			{
				m_position = m_objectBlock.CountBytes;
			}

			return m_position;
		}

		// Frees the blocks that are not used,
		// or expands the blocks.
		public override void SetLength(long value)
		{
			// This might need to change to support very, very large files.
			int blocks = (int)Math.Ceiling(value / (double)GODB.BLOCK_SIZE);
			int n = m_objectBlock.Blocks.Length;
			if (blocks < n)
			{
				// Cut down the object.
				long[] freeBlocks = new long[n - blocks];
				for (int i = blocks; i < n; i++)
					freeBlocks[i - blocks] = m_objectBlock.Blocks[i];
				m_odb.DeleteBlocks(freeBlocks);
				long[] newBlocks = new long[blocks];
				for (int i = 0; i < blocks; i++)
					newBlocks[i] = m_objectBlock.Blocks[i];
				m_objectBlock.Blocks = newBlocks;
			}
			else if (blocks > n)
			{
				// Expand object with new blocks.
				long lastPos = -1;
				if (n > 0)
					lastPos = m_objectBlock.Blocks[n - 1];
				long[] newPositions = m_odb.FindNewPos(blocks - n, lastPos);
				long[] positions = new long[blocks];
				for (int i = 0; i < n; i++)
					positions[i] = m_objectBlock.Blocks[i];
				for (int i = n; i < blocks; i++)
					positions[i] = newPositions[i - n];
				m_objectBlock.Blocks = positions;
			}
			m_objectBlock.CountBytes = (int)value;
		}

		// Writes the bytes into object.
		// It is expanded automatically if writing after the end.
		public override void Write(byte[] buffer, int offset, int count)
		{
			// Expand the length of stream if necessary.
			if (m_objectBlock.Blocks.Length <= (int)((m_position + count) / GODB.BLOCK_SIZE))
				SetLength(m_position + count);

			long filePos = this.ComputeOdbPos(m_position);
			m_odb.Stream.Seek(filePos, System.IO.SeekOrigin.Begin);
			int bytesLast = GODB.BLOCK_SIZE - (int)(filePos % GODB.BLOCK_SIZE);
			if (count < bytesLast)
			{
				m_odb.Stream.Write(buffer, offset, count);
				m_position += count;
			}
			else
			{
				// NOT USED: long newLength = m_position + count;
				
				int remaining = count;
				m_odb.Stream.Write(buffer, offset, bytesLast);
				m_position += bytesLast;
				offset += bytesLast;
				remaining -= bytesLast;

				while (remaining != 0)
				{
					filePos = this.ComputeOdbPos(m_position);
					m_odb.Stream.Seek(filePos, System.IO.SeekOrigin.Begin);
					// NOT USED: int block = (int)(m_position / ODB.BLOCK_SIZE);
					if (remaining > GODB.BLOCK_SIZE)
					{
						m_odb.Stream.Write(buffer, offset, GODB.BLOCK_SIZE);
						offset += GODB.BLOCK_SIZE;
						m_position += GODB.BLOCK_SIZE;
						remaining -= GODB.BLOCK_SIZE;
					}
					else
					{
						m_odb.Stream.Write(buffer, offset, remaining);
						offset += remaining;
						m_position += remaining;
						remaining = 0;
					}
				}
			}

			if (m_position > m_objectBlock.CountBytes)
				m_objectBlock.CountBytes = (int)m_position;
		}

		public override void Close()
		{
			m_odb.Stream.Flush();
		}

		public override void WriteByte(byte value)
		{
			this.Write(new byte[]{value}, 0, 1);
		}

		public override int ReadByte()
		{
			byte[] bytes = new byte[]{0};
			this.Read(bytes, 0, 1);
			return bytes[0];
		}

	}
	
	// 
	// (01.06.2007 13:23)
	// Allows undoing an operation
	// performed on the odb file.
	//
	// This is useful when having a user dialog where 
	// the user can press "Cancel" to not commit the changes done.
	//
	// Commit applies the changes.
	// Rollback undos the changes.
	//
	public class Transaction
	{
		public Transaction(OdbLib.GODB odb)
		{
			m_odb = odb;
		}

		public void Write(long oid, byte[] bytes)
		{
			long mapOID = -1;
			if (m_mapOIDs.ContainsKey(oid))
				mapOID = (long)m_mapOIDs[oid];
			else
			{
				mapOID = m_odb.GetNewOID();
				m_mapOIDs.Add(oid, mapOID);
			}

			m_odb.Write(mapOID, bytes);
		}

		public byte[] Read(long oid)
		{
			long mapOID = oid;
			// Use mapped OID if it exists.
			if (m_mapOIDs.ContainsKey(oid))
				mapOID = (long)m_mapOIDs[oid];

			return m_odb.Read(mapOID);
		}
		
		public void Delete(long oid)
		{
			long mapOID = -1;
			if (m_mapOIDs.ContainsKey(oid))
			{
				mapOID = m_mapOIDs[oid];
				m_odb.Delete(mapOID);
			}
			else
			{
				// Create a "false" deleted oid.
				mapOID = m_odb.GetNewOID();
				this.m_mapOIDs[oid] = mapOID;
			}
		}

		//
		// Applies the changes to the odb file.
		//
		public void Commit()
		{
			foreach (var entry in m_mapOIDs)
			{
				long oid = entry.Key;
				long mapOID = entry.Value;
				if (m_odb.Contains(mapOID))
				{
					// Move the new data in the old data position.
					byte[] bytes = m_odb.Read(mapOID);
					m_odb.Delete(mapOID);
					m_odb.Write(oid, bytes);
				}
				else
				{
					// Delete the old data.
					m_odb.Delete(oid);
				}
			}
		}

		// 
		// Undos the changes done to the odb file.
		//
		public void Rollback()
		{
			foreach (var entry in m_mapOIDs)
			{
				long mapOID = entry.Value;
				if (m_odb.Contains(mapOID))
				{
					// Delete the new data.
					m_odb.Delete(mapOID);
				}
			}
		}

		public long GetNewOID()
		{
			return m_odb.GetNewOID();
		}

		private Dictionary<long, long> m_mapOIDs = new Dictionary<long, long>();
		private OdbLib.GODB m_odb;
	}
	
	
	// Controls a base of binary blobs that are identified by name.
	public class BinaryDatabase
	{
		private GODB m_odb;
		private Hashtable m_ids = new Hashtable();
		
		private Hashtable m_newIds = new Hashtable();
		
		public BinaryDatabase(string filename, bool readOnly)
		{
			m_odb = new GODB(readOnly);
			m_odb.Open(filename);
			ReadIds();
		}
		
		/// <summary>
		/// This method find all files that where not added in this session.
		/// </summary>
		/// <returns></returns>
		public string[] UnusedFiles()
		{
			ArrayList list = new ArrayList();
			foreach (DictionaryEntry entry in m_ids)
			{
				string file = (string)entry.Key;
				if (!m_newIds.Contains(file))
					list.Add(file);
			}
			string[] table = new string[list.Count];
			list.CopyTo(table, 0);
			return table;
		}

		/// <summary>
		/// Reads the ids from file.
		/// </summary>
		private void ReadIds()
		{
			if (m_odb.Contains(GODB.ROOT_OID))
			{
				byte[] bytes = m_odb.Read(GODB.ROOT_OID);
				System.IO.MemoryStream mem = new System.IO.MemoryStream(bytes);
				System.IO.BinaryReader r = new System.IO.BinaryReader(mem);
				int n = r.ReadInt32();
				for (int i = 0; i < n; i++)
				{
					string file = r.ReadString();
					long id = r.ReadInt64();
					m_ids[file] = id;
				}
				r.Close();
			}
			else
				m_odb.Reserve(GODB.ROOT_OID);
		}

		/// <summary>
		/// Saves the ids to file.
		/// </summary>
		private void SaveIds()
		{
			// TESTING: Reading between multiple processes.
			// 1. Start two processes using the same picture database.
			// 2. Close one.
			// 3. Check that no error occured.
			// END TESTING
			// Don't write if opened readonly.
			if (!m_odb.ReadOnly)
			{
				System.IO.MemoryStream mem = new System.IO.MemoryStream();
				System.IO.BinaryWriter w = new System.IO.BinaryWriter(mem);
				w.Write(m_ids.Count);
				foreach (DictionaryEntry entry in m_ids)
				{
					string file = (string)entry.Key;
					long id = (long)entry.Value;
					w.Write(file);
					w.Write(id);
				}
				w.Flush();
				byte[] bytes = mem.ToArray();
				m_odb.Write(GODB.ROOT_OID, bytes);
				mem.Close();
			}
		}

		public void Close()
		{
			SaveIds();
			m_odb.Close();
		}
		
		public static byte[] Zip(byte[] data)
		{
			System.IO.MemoryStream stream = new System.IO.MemoryStream();
			System.IO.Compression.DeflateStream zStream = new System.IO.Compression.DeflateStream(
				stream, System.IO.Compression.CompressionMode.Compress, true);
			zStream.Write(data, 0, data.Length);
			zStream.Close();
			return stream.ToArray();
		}
		
		public static byte[] Unzip(byte[] data)
		{
			System.IO.Compression.CompressionMode mode = System.IO.Compression.CompressionMode.Decompress;
			System.IO.Compression.DeflateStream stream = new System.IO.Compression.DeflateStream(
				new System.IO.MemoryStream(data), mode);
			System.IO.MemoryStream mem = new System.IO.MemoryStream();
			byte[] buffer = new byte[4096];
			while (true)
			{
				int count = stream.Read(buffer, 0, buffer.Length);
				if (count != 0)
					mem.Write(buffer, 0, count);
				if (count != buffer.Length)
					break;
			}
			stream.Close();
			return mem.ToArray();
		}

		// Replaces existing bytes.
		public void AddBytes(string file, byte[] bytes, bool saveIds)
		{
			long oid;
			if (m_ids.Contains(file))
				oid = (long)m_ids[file];
			else
				oid = m_odb.GetNewOID();
			m_ids[file] = oid;
			m_newIds[file] = oid;

			m_odb.Write(oid, Zip(bytes));

			// Save oids after each change.
			if (saveIds)
			{
				this.SaveIds();
				m_odb.SaveOIDs();
			}
		}
		
		/// <summary>
		/// Returns true if file already exists in database, marking it as used
		/// if markAsUsed is true.
		/// </summary>
		/// <param name="file"></param>
		/// <param name="markAsUsed"></param>
		/// <returns></returns>
		public bool SkipThis(string file, bool markAsUsed)
		{
			if (m_ids.ContainsKey(file))
			{
				if (markAsUsed)
					m_newIds[file] = (long)m_ids[file];
				return true;
			}
			return false;
		}
		
		public void AddBytesUncompressed(string file, byte[] bytes, bool saveIds)
		{
			long oid;
			if (m_ids.Contains(file))
				oid = (long)m_ids[file];
			else
				oid = m_odb.GetNewOID();
			m_ids[file] = oid;
			m_newIds[file] = oid;

			m_odb.Write(oid, bytes);

			if (saveIds)
			{
				// Save oids after each change.
				this.SaveIds();
				m_odb.SaveOIDs();
			}
		}

		public void ImportFile(string file, string name, bool saveOids)
		{
			long oid;
			if (m_ids.Contains(file))
				oid = (long)m_ids[file];
			else
				oid = m_odb.GetNewOID();
			m_ids[name] = oid;
			m_newIds[name] = oid;

			System.IO.FileStream mem = new System.IO.FileStream(file, System.IO.FileMode.Open);
			byte[] bytes = new byte[mem.Length];
			mem.Read(bytes, 0, (int)mem.Length);
			mem.Close();

			m_odb.Write(oid, Zip(bytes));

			// Save oids after each change.
			if (saveOids)
			{
				this.SaveIds();
				m_odb.SaveOIDs();
			}
		}

		// Deletes something from the base.
		public void Delete(string file, bool saveOids)
		{
			long oid = (long)m_ids[file];
			m_odb.Delete(oid);

			if (saveOids)
			{
				// Save oids after each change.
				this.SaveIds();
				m_odb.SaveOIDs();
			}
		}

		public byte[] GetBytes(string file)
		{
			if (m_ids.Contains(file))
			{
				long oid = (long)m_ids[file];
				byte[] bytes = m_odb.Read(oid);
				return Unzip(bytes);
			}
			else
				return null;
		}
		
		public byte[] GetBytesUncompressed(string file)
		{
			if (m_ids.Contains(file))
			{
				long oid = (long)m_ids[file];
				byte[] bytes = m_odb.Read(oid);
				return bytes;
			}
			else
				return null;
		}
	}
}
